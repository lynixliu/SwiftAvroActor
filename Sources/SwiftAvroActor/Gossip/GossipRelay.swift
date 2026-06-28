import SwiftAvroCore
import SwiftAvroRpc

/// Propagates service registrations to all live cluster peers via one-way Avro IPC calls.
///
/// Create one `GossipRelay` per node, wire it to the same ``ClusterNode`` used for
/// SWIM membership, and inject it into ``GossipCatalog``. The relay:
///
/// - Tracks live peers by watching ``ClusterNode/events``.
/// - Derives each peer's gossip TCP port as `swimPort + gossipPortOffset` (default: +1).
/// - Sends a one-way `propagate` message to every live peer whenever a new service is registered.
/// - Maintains one persistent ``AvroIPCClient`` connection per peer and drops it on node failure.
public actor GossipRelay {

    private let rpc:              SwiftAvroRpc
    private let proto:            String
    private let hash:             MD5Hash
    private let gossipPortOffset: Int
    private let tls:              AvroTLSConfig?

    private var peers:     [String: Int]           = [:]  // nodeID → gossip TCP port
    private var clients:   [String: AvroIPCClient] = [:]
    private var watchTask: Task<Void, Never>?

    /// - Parameters:
    ///   - gossipPortOffset: Gossip TCP port = SWIM UDP port + this offset. Defaults to 1.
    ///   - tls: TLS settings for outbound gossip connections. `nil` (the default) uses plain TCP.
    public init(gossipPortOffset: Int = 1, tls: GossipTLS? = nil) {
        self.gossipPortOffset = gossipPortOffset
        self.proto            = GossipProtocol.json
        self.hash             = SwiftAvroRpc.md5Hash(of: GossipProtocol.json)
        self.rpc              = SwiftAvroRpc(threads: 1)
        self.tls              = tls?.client
    }

    /// Begins watching ``ClusterNode/events`` in a background task and updates the peer table.
    ///
    /// Call this once after creating the relay. The watch task is cancelled by ``shutdown()``.
    public func startWatching(node: ClusterNode) {
        let events = node.events   // nonisolated let — no await needed
        watchTask = Task { [weak self] in
            for await event in events {
                guard let self else { return }
                switch event {
                case .memberUp(let nodeID):   await self.add(peer: nodeID)
                case .memberDown(let nodeID): await self.drop(peer: nodeID)
                }
            }
        }
    }

    /// Sends `info` to all currently live peers as a one-way gossip message.
    ///
    /// Failures are silently swallowed — SWIM handles dead-node deregistration
    /// and the anti-entropy sync (if configured) will heal any gaps.
    public func relay(_ info: ServiceInfo) async {
        let wire = ServiceInfoWire(from: info)
        for (nodeID, port) in peers {
            await push(wire: wire, to: nodeID, gossipPort: port)
        }
    }

    /// Disconnects all peer connections and cancels the membership watch task.
    public func shutdown() async throws {
        watchTask?.cancel()
        watchTask = nil
        for client in clients.values { try? await client.disconnect() }
        clients.removeAll()
        peers.removeAll()
        try await rpc.stop()
    }

    /// Returns a random live peer's host and gossip port, or `nil` if no peers are known.
    ///
    /// Used by ``GossipCatalog`` to select a target for each anti-entropy sync round.
    public func randomPeer() -> (host: String, port: Int)? {
        guard let (nodeID, port) = peers.randomElement() else { return nil }
        let host = String(nodeID.split(separator: ":")[0])
        return (host, port)
    }

    // MARK: - Private

    private func add(peer nodeID: String) {
        // nodeID format: "<host>:<swimPort>"
        let parts = nodeID.split(separator: ":")
        guard parts.count == 2, let swimPort = Int(parts[1]) else { return }
        peers[nodeID] = swimPort + gossipPortOffset
    }

    private func drop(peer nodeID: String) async {
        peers.removeValue(forKey: nodeID)
        if let client = clients.removeValue(forKey: nodeID) {
            try? await client.disconnect()
        }
    }

    private func push(wire: ServiceInfoWire, to nodeID: String, gossipPort: Int) async {
        let host = String(nodeID.split(separator: ":")[0])
        do {
            let client = try await connection(to: nodeID, host: host, port: gossipPort)
            try await client.onewayCall(messageName: "propagate", parameters: [wire])
        } catch {
            // Drop the cached client so the next relay attempt reconnects.
            clients.removeValue(forKey: nodeID)
        }
    }

    private func connection(to nodeID: String, host: String, port: Int) async throws -> AvroIPCClient {
        if let existing = clients[nodeID] { return existing }
        let context = try await rpc.makeIPCContext()
        let client  = try await rpc.makeClient(AvroIPCClientConfig(
            transport:      TCPTransport(host: host, port: port),
            context:        context,
            clientHash:     hash,
            clientProtocol: proto,
            serverHash:     hash,
            tls:            tls
        ))
        clients[nodeID] = client
        return client
    }
}
