import SwiftAvroRpc

/// Watches cluster membership and deregisters services from dead nodes.
///
/// Run in a long-lived `Task` via ``watch()``. When SWIM marks a peer as
/// unreachable or dead, `HealthMonitor` removes all of its service endpoints
/// from the catalogue so stale entries are no longer returned to clients.
public actor HealthMonitor {

    private let events:    AsyncStream<ClusterNode.NodeEvent>
    private let catalogue: any ServiceCatalog

    public init(node: ClusterNode, catalogue: any ServiceCatalog) {
        self.events    = node.events
        self.catalogue = catalogue
    }

    /// Starts watching for membership changes. Suspends indefinitely — run inside a `Task`.
    public func watch() async {
        for await event in events {
            if case .memberDown(let nodeID) = event {
                try? await catalogue.deregister(nodeID: nodeID)
            }
        }
    }
}
