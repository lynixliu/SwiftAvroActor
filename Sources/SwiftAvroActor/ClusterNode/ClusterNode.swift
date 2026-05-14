@preconcurrency import NIO
@preconcurrency import SWIMNIOExample
@preconcurrency import SWIM
import ClusterMembership

/// Manages SWIM-based cluster membership for this process.
///
/// Every process in the system creates exactly one `ClusterNode`. It drives the
/// SWIM failure-detection protocol over UDP and broadcasts membership changes
/// via ``events``.
///
/// ```swift
/// let node = ClusterNode(host: "0.0.0.0", port: 8080)
/// await node.addSeed(host: "seed.host", port: 8081)
/// try await node.start()
/// ```
public actor ClusterNode {

    /// Stable identifier for this node: `"\(host):\(port)"`.
    public nonisolated let nodeID: String

    /// Stream of membership change events. Iterate from a long-lived `Task`
    /// to react to nodes joining or leaving the cluster.
    public nonisolated let events: AsyncStream<NodeEvent>

    private let host: String
    private let port: Int
    private let group: MultiThreadedEventLoopGroup
    private var swimSettings: SWIMNIO.Settings
    private var channel: (any Channel)?
    private nonisolated let eventsContinuation: AsyncStream<NodeEvent>.Continuation

    /// Creates a cluster node for `host:port`. Call ``addSeed(_:port:)`` for each
    /// known peer, then ``start()`` to bind the UDP socket and begin SWIM gossip.
    public init(host: String, port: Int) {
        self.host   = host
        self.port   = port
        self.nodeID = "\(host):\(port)"
        self.group  = MultiThreadedEventLoopGroup(numberOfThreads: 1)

        var swimSettings = SWIM.Settings()
        swimSettings.node = ClusterMembership.Node(
            protocol: "udp",
            host: host,
            port: port,
            uid: .random(in: .min ... .max)
        )
        self.swimSettings = SWIMNIO.Settings(swim: swimSettings)

        (self.events, self.eventsContinuation) = AsyncStream<NodeEvent>.makeStream()
    }

    /// Registers a peer as an initial SWIM contact point.
    /// Must be called before ``start()``.
    public func addSeed(host: String, port: Int) {
        let node = ClusterMembership.Node(protocol: "udp", host: host, port: port, uid: nil)
        swimSettings.swim.initialContactPoints.insert(node)
    }

    /// Binds the UDP socket and begins SWIM gossip.
    public func start() async throws {
        // Capture values before crossing isolation boundary into the channelInitializer closure.
        let continuation                 = eventsContinuation
        nonisolated(unsafe) let settings = swimSettings
        let host                         = self.host
        let port                         = self.port

        let ch = try await DatagramBootstrap(group: group)
            .channelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .channelInitializer { channel in
                channel.pipeline
                    .addHandler(SWIMNIOHandler(settings: settings))
                    .flatMap {
                        channel.pipeline.addHandler(
                            SWIMEventBridge(continuation: continuation)
                        )
                    }
            }
            .bind(host: host, port: port)
            .get()
        self.channel = ch
    }

    /// Shuts down SWIM and releases the event loop group.
    public func shutdown() async throws {
        eventsContinuation.finish()
        try await channel?.close()
        try await group.shutdownGracefully()
    }
}

// MARK: - NodeEvent

extension ClusterNode {
    /// A membership change event emitted by SWIM failure detection.
    public enum NodeEvent: Sendable {
        /// A node has become reachable (new member, or recovered from unreachable).
        case memberUp(nodeID: String)
        /// A node has become unreachable or is confirmed dead.
        case memberDown(nodeID: String)
    }
}

// MARK: - NIO bridge handler

/// Translates `SWIM.MemberStatusChangedEvent` pipeline events to ``ClusterNode/NodeEvent``
/// and yields them into the ``ClusterNode/events`` async stream.
private final class SWIMEventBridge: ChannelInboundHandler, @unchecked Sendable {
    typealias InboundIn = SWIM.MemberStatusChangedEvent<SWIM.NIOPeer>

    private let continuation: AsyncStream<ClusterNode.NodeEvent>.Continuation

    init(continuation: AsyncStream<ClusterNode.NodeEvent>.Continuation) {
        self.continuation = continuation
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let change = unwrapInboundIn(data)
        guard change.isReachabilityChange else { return }

        let nodeID = "\(change.member.node.host):\(change.member.node.port)"
        let event: ClusterNode.NodeEvent = (change.status.isUnreachable || change.status.isDead)
            ? .memberDown(nodeID: nodeID)
            : .memberUp(nodeID: nodeID)
        continuation.yield(event)
    }

    func channelInactive(context: ChannelHandlerContext) {
        continuation.finish()
        context.fireChannelInactive()
    }
}
