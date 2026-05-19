import Testing
import Foundation
import SwiftAvroCore
import SwiftAvroRpc
@testable import SwiftAvroActor

// MARK: - Service test helpers

private func withServiceServer<S: AvroService>(
    service: S,
    port: Int,
    _ body: () async throws -> Void
) async throws {
    let rpc  = SwiftAvroRpc(threads: 1)
    let ctx  = try await rpc.makeIPCContext()
    let hash = SwiftAvroRpc.md5Hash(of: service.avroProtocol)
    let ch   = try await rpc.makeServer(AvroIPCServerConfig(
        transport:      TCPTransport(host: "127.0.0.1", port: port),
        context:        ctx,
        serverHash:     hash,
        serverProtocol: service.avroProtocol,
        handler:        service.handler
    ))
    do {
        try await body()
        try await ch.close()
        try await rpc.stop()
    } catch {
        try? await ch.close()
        try? await rpc.stop()
        throw error
    }
}

private func withServiceClient(
    proto: String,
    port: Int,
    _ body: (AvroIPCClient) async throws -> Void
) async throws {
    let rpc    = SwiftAvroRpc(threads: 1)
    let ctx    = try await rpc.makeIPCContext()
    let hash   = SwiftAvroRpc.md5Hash(of: proto)
    let client = try await rpc.makeClient(AvroIPCClientConfig(
        transport:      TCPTransport(host: "127.0.0.1", port: port),
        context:        ctx,
        clientHash:     hash,
        clientProtocol: proto,
        serverHash:     hash
    ))
    do {
        try await body(client)
        try? await client.disconnect()
        try? await rpc.stop()
    } catch {
        try? await client.disconnect()
        try? await rpc.stop()
        throw error
    }
}

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

// MARK: - DocumentSyncService codec tests (no TCP)

@Suite("DocumentSyncService")
struct DocumentSyncServiceTests {

    @Test("DocumentEventWire round-trips through Avro codec")
    func documentEventWireCodecRoundTrip() throws {
        let original = DocumentEventWire(eventJson: #"{"id":"abc","lamport":42}"#)
        let avro = Avro()
        let encoded: Data = try avro.encodeFrom(original, schema: DocumentSyncProtocol.eventWireSchema)
        let decoded: DocumentEventWire = try avro.decodeFrom(from: encoded, schema: DocumentSyncProtocol.eventWireSchema)
        #expect(decoded == original)
    }

    @Test("SyncRangeRequest round-trips through Avro codec")
    func syncRangeRequestCodecRoundTrip() throws {
        let req = SyncRangeRequest(fromLamport: 10, toLamport: 99)
        let avro = Avro()
        let encoded: Data = try avro.encodeFrom(req, schema: DocumentSyncProtocol.syncRangeRequestSchema)
        let decoded: SyncRangeRequest = try avro.decodeFrom(from: encoded, schema: DocumentSyncProtocol.syncRangeRequestSchema)
        #expect(decoded == req)
    }

    @Test("SyncRangeResponse with two events round-trips through Avro codec")
    func syncRangeResponseCodecRoundTrip() throws {
        let resp = SyncRangeResponse(events: [
            DocumentEventWire(eventJson: #"{"id":"e1","lamport":1}"#),
            DocumentEventWire(eventJson: #"{"id":"e2","lamport":2}"#),
        ])
        let avro = Avro()
        let encoded: Data = try avro.encodeFrom(resp, schema: DocumentSyncProtocol.syncRangeResponseSchema)
        let decoded: SyncRangeResponse = try avro.decodeFrom(from: encoded, schema: DocumentSyncProtocol.syncRangeResponseSchema)
        #expect(decoded == resp)
    }

    @Test("empty SyncRangeResponse encodes and decodes")
    func emptySyncRangeResponseCodecRoundTrip() throws {
        let resp = SyncRangeResponse(events: [])
        let avro = Avro()
        let encoded: Data = try avro.encodeFrom(resp, schema: DocumentSyncProtocol.syncRangeResponseSchema)
        let decoded: SyncRangeResponse = try avro.decodeFrom(from: encoded, schema: DocumentSyncProtocol.syncRangeResponseSchema)
        #expect(decoded.events.isEmpty)
    }

    @Test("pushEvent handler yields event JSON to receivedEvents stream")
    func pushEventHandlerYields() async throws {
        let service = DocumentSyncService()
        let json    = #"{"id":"evt-1","lamport":7}"#
        let wire    = DocumentEventWire(eventJson: json)

        // The stream await must live inside withServiceServer so the server
        // stays open long enough to deliver the one-way message.
        var received: String?
        try await withServiceServer(service: service, port: 9790) {
            try await withServiceClient(proto: DocumentSyncProtocol.json, port: 9790) { client in
                try await client.onewayCall(messageName: "pushEvent", parameters: [wire])
            }
            for await ev in service.receivedEvents {
                received = ev
                break
            }
        }
        #expect(received == json)
    }

    @Test("syncRange handler returns events from provider")
    func syncRangeHandlerReturnsPeerevents() async throws {
        let service = DocumentSyncService()
        let stored  = [#"{"id":"e1","lamport":1}"#, #"{"id":"e2","lamport":2}"#]
        await service.setSyncProvider { _, _ in stored }

        let req = SyncRangeRequest(fromLamport: 0, toLamport: 10)
        var response: SyncRangeResponse?

        try await withServiceServer(service: service, port: 9791) {
            try await withServiceClient(proto: DocumentSyncProtocol.json, port: 9791) { client in
                response = try await client.call(
                    messageName: "syncRange",
                    parameters:  [req],
                    as:          SyncRangeResponse.self
                )
            }
        }

        #expect(response?.events.count == 2)
        #expect(response?.events.map(\.eventJson) == stored)
    }

    @Test("syncRange handler returns empty array when no provider is set")
    func syncRangeNoProvider() async throws {
        let service = DocumentSyncService()
        var response: SyncRangeResponse?

        try await withServiceServer(service: service, port: 9792) {
            try await withServiceClient(proto: DocumentSyncProtocol.json, port: 9792) { client in
                response = try await client.call(
                    messageName: "syncRange",
                    parameters:  [SyncRangeRequest(fromLamport: 0, toLamport: 100)],
                    as:          SyncRangeResponse.self
                )
            }
        }
        #expect(response?.events.isEmpty == true)
    }
}

// MARK: - CursorPresenceService codec tests

@Suite("CursorPresenceService")
struct CursorPresenceServiceTests {

    @Test("CursorUpdate round-trips through Avro codec")
    func cursorUpdateCodecRoundTrip() throws {
        let update  = CursorUpdate(blockId: "§p_0000_001", actorId: "alice")
        let avro    = Avro()
        let encoded: Data = try avro.encodeFrom(update, schema: CursorPresenceProtocol.cursorUpdateSchema)
        let decoded: CursorUpdate = try avro.decodeFrom(from: encoded, schema: CursorPresenceProtocol.cursorUpdateSchema)
        #expect(decoded == update)
    }

    @Test("updateCursor handler yields CursorUpdate to receivedUpdates stream")
    func updateCursorHandlerYields() async throws {
        let service = CursorPresenceService()
        let update  = CursorUpdate(blockId: "§h_0000_002", actorId: "bob")

        var received: CursorUpdate?
        try await withServiceServer(service: service, port: 9800) {
            try await withServiceClient(proto: CursorPresenceProtocol.json, port: 9800) { client in
                try await client.onewayCall(messageName: "updateCursor", parameters: [update])
            }
            for await ev in service.receivedUpdates {
                received = ev
                break
            }
        }
        #expect(received == update)
    }

    @Test("unknown message name is silently ignored")
    func unknownMessageIgnored() throws {
        let handler = CursorPresenceHandler(continuation: AsyncStream<CursorUpdate>.makeStream().continuation)
        Task {
            let result = try await handler.handle(messageName: "bogus", requestData: Data())
            #expect(result.isEmpty)
        }
    }
}

// MARK: - DocumentCompileService codec tests

@Suite("DocumentCompileService")
struct DocumentCompileServiceTests {

    @Test("CompileRequest round-trips through Avro codec")
    func compileRequestCodecRoundTrip() throws {
        let req = CompileRequest(
            texPath: "/tmp/doc.tex", outputDir: "/tmp",
            callbackNodeId: "127.0.0.1:9810",
            callbackHost: "127.0.0.1", callbackPort: 9811
        )
        let avro    = Avro()
        let encoded: Data = try avro.encodeFrom(req, schema: DocumentCompileProtocol.compileRequestSchema)
        let decoded: CompileRequest = try avro.decodeFrom(from: encoded, schema: DocumentCompileProtocol.compileRequestSchema)
        #expect(decoded == req)
    }

    @Test("CompileEventWire round-trips through Avro codec")
    func compileEventWireCodecRoundTrip() throws {
        let event   = CompileEventWire(kind: "success", text: "", pdfPath: "/tmp/doc.pdf")
        let avro    = Avro()
        let encoded: Data = try avro.encodeFrom(event, schema: CompileCallbackProtocol.compileEventSchema)
        let decoded: CompileEventWire = try avro.decodeFrom(from: encoded, schema: CompileCallbackProtocol.compileEventSchema)
        #expect(decoded == event)
    }

    @Test("compile handler delivers CompileRequest to registered handler")
    func compileHandlerDeliversRequest() async throws {
        let service = DocumentCompileService()

        // Route received requests through a stream so the test can await deterministically.
        let (stream, cont) = AsyncStream<CompileRequest>.makeStream()
        await service.setCompileHandler { req in cont.yield(req) }

        let req = CompileRequest(
            texPath: "/tmp/test.tex", outputDir: "/tmp",
            callbackNodeId: "127.0.0.1:9812",
            callbackHost: "127.0.0.1", callbackPort: 9812
        )

        var received: CompileRequest?
        try await withServiceServer(service: service, port: 9810) {
            try await withServiceClient(proto: DocumentCompileProtocol.serviceJson, port: 9810) { client in
                try await client.onewayCall(messageName: "compile", parameters: [req])
            }
            for await r in stream {
                received = r
                break
            }
        }
        #expect(received == req)
    }

    @Test("callback handler delivers CompileEventWire to receivedEvents stream")
    func callbackHandlerDeliversEvent() async throws {
        let cbService = CompileCallbackService()
        let event     = CompileEventWire(kind: "stdout", text: "Latexmk: Run 1", pdfPath: "")

        try await withServiceServer(service: cbService, port: 9820) {
            try await withServiceClient(proto: CompileCallbackProtocol.json, port: 9820) { client in
                try await client.onewayCall(messageName: "onEvent", parameters: [event])
            }
        }

        var received: CompileEventWire?
        for await ev in cbService.receivedEvents {
            received = ev
            break
        }
        #expect(received == event)
    }
}
