import Testing
import Foundation
import SwiftAvroCore
import SwiftAvroRpc
@testable import SwiftAvroActor

// MARK: - Fixtures

private func makeInfo(
    name: String,
    nodeID: String,
    version: String = "1.0",
    port: Int = 9090
) -> ServiceInfo {
    ServiceInfo(name: name, version: version,
                endpoint: .tcp(host: "127.0.0.1", port: port), nodeID: nodeID)
}

// MARK: - Gossip server lifecycle helper

/// Starts a gossip TCP server on `host:port`, runs `body`, then shuts down cleanly.
private func withGossipServer(
    port: Int,
    services: [ServiceInfo] = [],
    _ body: (GossipCatalog, ServiceRegistry) async throws -> Void
) async throws {
    let registry = ServiceRegistry()
    for svc in services { await registry.register(svc) }
    let catalog  = GossipCatalog(local: registry, relay: GossipRelay())
    _ = try await catalog.startGossipServer(host: "127.0.0.1", port: port)
    do {
        try await body(catalog, registry)
        try await catalog.shutdown()
    } catch {
        try? await catalog.shutdown()
        throw error
    }
}

// MARK: - ServiceRegistry

@Suite("ServiceRegistry")
struct ServiceRegistryTests {

    @Test("register and discover a service")
    func registerAndDiscover() async {
        let registry = ServiceRegistry()
        let info = makeInfo(name: "greeter", nodeID: "127.0.0.1:9710")
        await registry.register(info)
        let found = await registry.discover(serviceName: "greeter")
        #expect(found == [info])
    }

    @Test("discover returns empty for unknown service")
    func discoverUnknown() async {
        let registry = ServiceRegistry()
        let found = await registry.discover(serviceName: "unknown")
        #expect(found.isEmpty)
    }

    @Test("deregister removes all endpoints for a node")
    func deregister() async {
        let registry = ServiceRegistry()
        let i1 = makeInfo(name: "svc", nodeID: "n1", port: 1)
        let i2 = makeInfo(name: "svc", nodeID: "n2", port: 2)
        await registry.register(i1)
        await registry.register(i2)
        await registry.deregister(nodeID: "n1")
        let found = await registry.discover(serviceName: "svc")
        #expect(found == [i2])
    }

    @Test("allEntries returns every registered service")
    func allEntries() async {
        let registry = ServiceRegistry()
        await registry.register(makeInfo(name: "auth",    nodeID: "n1"))
        await registry.register(makeInfo(name: "billing", nodeID: "n1"))
        await registry.register(makeInfo(name: "search",  nodeID: "n2"))
        let all = await registry.allEntries()
        #expect(all.count == 3)
    }
}

// MARK: - ClusterNode

@Suite("ClusterNode")
struct ClusterNodeTests {

    @Test("nodeID is formatted as host:port")
    func nodeIDFormat() {
        let node = ClusterNode(host: "127.0.0.1", port: 9720)
        #expect(node.nodeID == "127.0.0.1:9720")
    }

    @Test("events stream is accessible without await")
    func eventsAccessible() {
        let node = ClusterNode(host: "127.0.0.1", port: 9721)
        _ = node.events   // nonisolated let — must not require await
    }
}

// MARK: - HealthMonitor

@Suite("HealthMonitor")
struct HealthMonitorTests {

    @Test("initialises with node and catalogue")
    func initSucceeds() {
        let node    = ClusterNode(host: "127.0.0.1", port: 9730)
        let monitor = HealthMonitor(node: node, catalogue: ServiceRegistry())
        _ = monitor
    }
}

// MARK: - Gossip: sync round trip

@Suite("GossipSync")
struct GossipSyncTests {

    // Builds a connected AvroIPCClient using the gossip protocol.
    private func makeSyncClient(port: Int) async throws -> (AvroIPCClient, SwiftAvroRpc) {
        let proto  = GossipProtocol.json
        let hash   = SwiftAvroRpc.md5Hash(of: proto)
        let rpc    = SwiftAvroRpc(threads: 1)
        let ctx    = try await rpc.makeIPCContext()
        let client = try await rpc.makeClient(AvroIPCClientConfig(
            transport:      TCPTransport(host: "127.0.0.1", port: port),
            context:        ctx,
            clientHash:     hash,
            clientProtocol: proto,
            serverHash:     hash
        ))
        return (client, rpc)
    }

    // Runs `body` with a connected client, then disconnects cleanly regardless of outcome.
    private func withSyncClient<T: Sendable>(
        port: Int,
        _ body: (AvroIPCClient) async throws -> T
    ) async throws -> T {
        let (client, rpc) = try await makeSyncClient(port: port)
        do {
            let result = try await body(client)
            try? await client.disconnect()
            try? await rpc.stop()
            return result
        } catch {
            try? await client.disconnect()
            try? await rpc.stop()
            throw error
        }
    }

    // MARK: Direct codec tests (no TCP)

    @Test("syncResponseSchema parses with ServiceInfoWire fields intact")
    func syncResponseSchemaStructure() throws {
        let schema = GossipProtocol.syncResponseSchema
        guard case .recordSchema(let outer) = schema else {
            #expect(Bool(false), "not a record schema"); return
        }
        guard let entriesField = outer.fields.first(where: { $0.name == "entries" }) else {
            #expect(Bool(false), "no entries field"); return
        }
        guard case .arraySchema(let arr) = entriesField.type else {
            #expect(Bool(false), "entries is not array: \(entriesField.type)"); return
        }
        guard case .recordSchema(let inner) = arr.items else {
            #expect(Bool(false), "items is not record: \(arr.items)"); return
        }
        let portField = inner.fields.first(where: { $0.name == "endpointPort" })
        #expect(portField != nil, "endpointPort field missing from inner ServiceInfoWire")
        #expect(portField?.type.isLong() == true, "endpointPort schema is not long: \(String(describing: portField?.type))")
    }

    @Test("ServiceInfoWire array encodes directly with array schema")
    func serviceInfoWireArrayEncoding() throws {
        let info = makeInfo(name: "billing", nodeID: "node-A", version: "2.1", port: 8888)
        let wire = ServiceInfoWire(from: info)
        let avro = Avro()
        // Schema is just an array of ServiceInfoWire — no SyncResponse wrapper
        let schema = avro.newSchema(schema: """
        {"type": "array", "items": {
          "type": "record", "name": "ServiceInfoWire",
          "fields": [
            {"name": "name",         "type": "string"},
            {"name": "version",      "type": "string"},
            {"name": "nodeID",       "type": "string"},
            {"name": "endpointType", "type": "string"},
            {"name": "endpointHost", "type": "string"},
            {"name": "endpointPort", "type": "long"},
            {"name": "endpointPath", "type": "string"}
          ]
        }}
        """)!
        let encoded: Data = try avro.encodeFrom([wire], schema: schema)
        #expect(encoded.count > 0)
    }

    @Test("SyncResponse with one entry encodes and decodes without IPC")
    func syncResponseCodecRoundTrip() throws {
        let info = makeInfo(name: "billing", nodeID: "node-A", version: "2.1", port: 8888)
        let wire = ServiceInfoWire(from: info)
        let response = SyncResponse(entries: [wire])
        let avro = Avro()
        let encoded: Data = try avro.encodeFrom(response, schema: GossipProtocol.syncResponseSchema)
        let decoded: SyncResponse = try avro.decodeFrom(from: encoded, schema: GossipProtocol.syncResponseSchema)
        #expect(decoded.entries.count == 1)
        #expect(decoded.entries[0].toServiceInfo() == info)
    }

    @Test("SyncRequest with one DigestEntry encodes and decodes without IPC")
    func syncRequestCodecRoundTrip() throws {
        let entry = DigestEntry(name: "auth", nodeID: "node-A", version: "1.0")
        let request = SyncRequest(digest: [entry])
        let avro = Avro()
        let encoded: Data = try avro.encodeFrom(request, schema: GossipProtocol.syncRequestSchema)
        let decoded: SyncRequest = try avro.decodeFrom(from: encoded, schema: GossipProtocol.syncRequestSchema)
        #expect(decoded.digest.count == 1)
        #expect(decoded.digest[0] == entry)
    }

    // MARK: Tests

    /// Core case: server has A, B, C; initiator sends digest containing only A;
    /// expects B and C back.
    @Test("sync returns entries the initiator is missing")
    func syncReturnsMissingEntries() async throws {
        let serverServices = [
            makeInfo(name: "auth",    nodeID: "node-A"),
            makeInfo(name: "billing", nodeID: "node-A"),
            makeInfo(name: "search",  nodeID: "node-B"),
        ]
        try await withGossipServer(port: 9780, services: serverServices) { _, _ in
            try await withSyncClient(port: 9780) { client in
                let digest = [DigestEntry(name: "auth", nodeID: "node-A", version: "1.0")]
                let response: SyncResponse = try await client.call(
                    messageName: "sync",
                    parameters:  [SyncRequest(digest: digest)],
                    as:          SyncResponse.self
                )
                let returned = Set(response.entries.map { "\($0.name)/\($0.nodeID)" })
                #expect(returned == ["billing/node-A", "search/node-B"])
            }
        }
    }

    /// Initiator already holds the entire catalog — server should return nothing.
    @Test("sync returns nothing when initiator is fully up to date")
    func syncFullyUpToDate() async throws {
        try await withGossipServer(port: 9781, services: [makeInfo(name: "auth", nodeID: "node-A")]) { _, _ in
            try await withSyncClient(port: 9781) { client in
                let digest = [DigestEntry(name: "auth", nodeID: "node-A", version: "1.0")]
                let response: SyncResponse = try await client.call(
                    messageName: "sync",
                    parameters:  [SyncRequest(digest: digest)],
                    as:          SyncResponse.self
                )
                #expect(response.entries.isEmpty)
            }
        }
    }

    /// Empty digest — initiator has nothing, should receive every entry.
    /// Also verifies the wire type converts back to an identical ``ServiceInfo``.
    @Test("sync response entries round-trip to ServiceInfo correctly")
    func syncResponseRestoresServiceInfo() async throws {
        let original = makeInfo(name: "billing", nodeID: "node-A", version: "2.1", port: 8888)
        try await withGossipServer(port: 9782, services: [original]) { _, _ in
            try await withSyncClient(port: 9782) { client in
                let response: SyncResponse = try await client.call(
                    messageName: "sync",
                    parameters:  [SyncRequest(digest: [])],
                    as:          SyncResponse.self
                )
                #expect(response.entries.count == 1)
                #expect(response.entries[0].toServiceInfo() == original)
            }
        }
    }

    /// Server catalog is empty — response should always be empty regardless of digest.
    @Test("sync against empty server returns empty response")
    func syncEmptyServer() async throws {
        try await withGossipServer(port: 9783) { _, _ in
            try await withSyncClient(port: 9783) { client in
                let response: SyncResponse = try await client.call(
                    messageName: "sync",
                    parameters:  [SyncRequest(digest: [])],
                    as:          SyncResponse.self
                )
                #expect(response.entries.isEmpty)
            }
        }
    }

    /// Applying the sync response to a local registry heals the missing entries.
    @Test("applying sync response registers missing entries into the local registry")
    func syncHealsLocalRegistry() async throws {
        let serverServices = [
            makeInfo(name: "auth",    nodeID: "node-A"),
            makeInfo(name: "billing", nodeID: "node-A"),
        ]
        try await withGossipServer(port: 9784, services: serverServices) { _, _ in
            let localReg = ServiceRegistry()
            await localReg.register(makeInfo(name: "auth", nodeID: "node-A"))

            try await withSyncClient(port: 9784) { client in
                let localDigest = await localReg.allEntries().map {
                    DigestEntry(name: $0.name, nodeID: $0.nodeID, version: $0.version)
                }
                let response: SyncResponse = try await client.call(
                    messageName: "sync",
                    parameters:  [SyncRequest(digest: localDigest)],
                    as:          SyncResponse.self
                )
                for wire in response.entries { await localReg.register(wire.toServiceInfo()) }
            }

            let auth    = await localReg.discover(serviceName: "auth")
            let billing = await localReg.discover(serviceName: "billing")
            #expect(auth.count == 1)
            #expect(billing.count == 1)
        }
    }
}
