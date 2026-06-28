import SwiftAvroRpc

/// Service catalogue that propagates every registration to all live cluster peers.
///
/// `GossipCatalog` is the user-facing ``ServiceCatalog`` implementation for multi-node
/// deployments. It wraps a local ``ServiceRegistry`` and a ``GossipRelay``:
///
/// - **Register** — writes to the local registry AND relays to all live peers via one-way
///   Avro IPC calls. Incoming gossip from peers writes directly to the local registry
///   (bypassing relay) so there are no re-gossip loops.
/// - **Discover / Deregister** — delegate to the local registry.
///
/// **Typical setup:**
/// ```swift
/// let node     = ClusterNode(host: host, port: 8080)
/// let registry = ServiceRegistry()
/// let relay    = GossipRelay()
/// let catalog  = GossipCatalog(local: registry, relay: relay)
///
/// // Start inbound gossip server (port = swim port + 1 by default)
/// try await catalog.startGossipServer(port: 8081)
///
/// // Wire membership events → relay peer table
/// await relay.startWatching(node: node)
///
/// // Wire membership events → health monitor
/// let monitor = HealthMonitor(node: node, catalogue: catalog)
/// Task { await monitor.watch() }
///
/// // Seed and start SWIM
/// await node.addSeed(host: seedHost, port: 8080)
/// try await node.start()
///
/// // Use catalog for service providers and clients
/// let provider = ServiceProvider(nodeID: node.nodeID)
/// try await provider.host(service: myService, endpoint: .tcp(host: host, port: 9000), catalogue: catalog)
/// ```
public actor GossipCatalog: ServiceCatalog {

    private let local:            ServiceRegistry
    private let relay:            GossipRelay
    private let rpc:              SwiftAvroRpc
    private let tls:              GossipTLS?
    private var server:           AvroServerChannel?
    private var antiEntropyTask:  Task<Void, Never>?

    /// - Parameters:
    ///   - local: The local service registry backing this catalog.
    ///   - relay: The gossip relay used to propagate registrations to peers.
    ///   - tls: TLS settings for the inbound gossip server and anti-entropy clients.
    ///     `nil` (the default) uses plain TCP. Use the same `GossipTLS` you pass to `relay`.
    public init(local: ServiceRegistry, relay: GossipRelay, tls: GossipTLS? = nil) {
        self.local = local
        self.relay = relay
        self.rpc   = SwiftAvroRpc(threads: 1)
        self.tls   = tls
    }

    // MARK: - Gossip server lifecycle

    /// Binds the inbound gossip TCP server on the given port.
    ///
    /// Must be called before remote peers can propagate registrations to this node.
    /// The port should equal the SWIM UDP port + `GossipRelay.gossipPortOffset` so
    /// peers can derive it automatically.
    @discardableResult
    public func startGossipServer(host: String = "0.0.0.0", port: Int) async throws -> AvroServerChannel {
        let proto   = GossipProtocol.json
        let hash    = SwiftAvroRpc.md5Hash(of: proto)
        let context = try await rpc.makeIPCContext()
        let channel = try await rpc.makeServer(AvroIPCServerConfig(
            transport:      TCPTransport(host: host, port: port),
            context:        context,
            serverHash:     hash,
            serverProtocol: proto,
            handler:        GossipHandler(registry: local),
            tls:            tls?.server
        ))
        server = channel
        return channel
    }

    /// Closes the gossip server and its event loop group.
    public func shutdown() async throws {
        antiEntropyTask?.cancel()
        antiEntropyTask = nil
        try? await server?.close()
        server = nil
        try await rpc.stop()
    }

    // MARK: - Anti-entropy sync

    /// Starts a background task that periodically picks a random peer, sends it the local
    /// catalog digest, and registers any entries the peer returns that this node is missing.
    ///
    /// Call this after ``startGossipServer(host:port:)`` and ``GossipRelay/startWatching(node:)``.
    /// Cancelled automatically by ``shutdown()``.
    ///
    /// - Parameter interval: How often to run a sync round. Defaults to 30 seconds.
    public func startAntiEntropySync(interval: Duration = .seconds(30)) {
        let registry = self.local
        let relay    = self.relay
        let proto    = GossipProtocol.json
        let hash     = SwiftAvroRpc.md5Hash(of: proto)
        antiEntropyTask = Task { [weak self] in
            while !Task.isCancelled {
                try? await Task.sleep(for: interval)
                guard !Task.isCancelled, let self else { break }
                guard let (host, port) = await relay.randomPeer() else { continue }
                let allEntries   = await registry.allEntries()
                let localDigest  = allEntries.map {
                    DigestEntry(name: $0.name, nodeID: $0.nodeID, version: $0.version)
                }
                do {
                    let context = try await self.rpc.makeIPCContext()
                    let client  = try await self.rpc.makeClient(AvroIPCClientConfig(
                        transport:      TCPTransport(host: host, port: port),
                        context:        context,
                        clientHash:     hash,
                        clientProtocol: proto,
                        serverHash:     hash,
                        tls:            self.tls?.client
                    ))
                    let response: SyncResponse = try await client.call(
                        messageName: "sync",
                        parameters:  [SyncRequest(digest: localDigest)],
                        as:          SyncResponse.self
                    )
                    try? await client.disconnect()
                    for wire in response.entries {
                        await registry.register(wire.toServiceInfo())
                    }
                } catch {
                    // Sync failures are non-fatal — the next round will retry.
                }
            }
        }
    }

    // MARK: - ServiceCatalog

    /// Registers `info` locally and relays it to all live peers.
    public func register(_ info: ServiceInfo) async throws {
        await local.register(info)
        await relay.relay(info)
    }

    /// Returns all live endpoints for the named service from the local view.
    public func discover(serviceName: String) async throws -> [ServiceInfo] {
        await local.discover(serviceName: serviceName)
    }

    /// Removes all endpoints for a node from the local view.
    /// Called by ``HealthMonitor`` on node failure — no gossip needed since every node
    /// independently receives the same SWIM down event.
    public func deregister(nodeID: String) async throws {
        await local.deregister(nodeID: nodeID)
    }
}
