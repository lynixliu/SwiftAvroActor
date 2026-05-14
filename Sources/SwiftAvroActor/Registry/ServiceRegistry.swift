import SwiftAvroRpc

/// In-memory service catalogue. Conforms to ``ServiceCatalog`` as a regular actor.
///
/// One `ServiceRegistry` runs per cluster node. Entries are added by ``ServiceProvider``
/// on startup and removed by ``HealthMonitor`` when SWIM detects node failure.
///
/// `ServiceRegistry` conforms to ``ServiceCatalog`` so it can be passed directly
/// to ``ServiceProvider``, ``ServiceClient``, and ``HealthMonitor``.
public actor ServiceRegistry: ServiceCatalog {

    private var services:  [String: [ServiceInfo]] = [:]
    /// Reverse index: nodeID → set of service names registered from that node.
    /// Keeps ``deregister(nodeID:)`` O(services per node) instead of O(total entries).
    private var nodeIndex: [String: Set<String>]   = [:]

    public init() {}

    /// Registers a service endpoint. Called by ``ServiceProvider`` on startup.
    public func register(_ info: ServiceInfo) {
        services[info.name, default: []].append(info)
        nodeIndex[info.nodeID, default: []].insert(info.name)
    }

    /// Removes all endpoints belonging to a node. Called by ``HealthMonitor`` on node failure.
    public func deregister(nodeID: String) {
        guard let names = nodeIndex.removeValue(forKey: nodeID) else { return }
        for name in names {
            services[name]?.removeAll { $0.nodeID == nodeID }
            if services[name]?.isEmpty == true { services.removeValue(forKey: name) }
        }
    }

    /// Returns all live endpoints for the named service.
    public func discover(serviceName: String) -> [ServiceInfo] {
        services[serviceName] ?? []
    }

    /// Returns every registered ``ServiceInfo`` — used by anti-entropy sync to build a digest.
    public func allEntries() -> [ServiceInfo] {
        services.values.flatMap { $0 }
    }
}
