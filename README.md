# SwiftAvroActor

A distributed microservice framework for Swift. Services are Swift actors, the wire protocol is [Apache Avro IPC](https://avro.apache.org/docs/current/spec.html#Protocol+Declaration), and cluster membership uses [SWIM gossip](https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf) via [swift-distributed-actors](https://github.com/apple/swift-distributed-actors).

Built on top of [SwiftAvroRpc](../SwiftAvroRpc) and [SwiftAvroCore](../SwiftAvroCore).

---

## Architecture

The system supports two node types. Both join the same cluster and share the same registry. Server nodes (macOS/Linux) host services over Unix domain sockets (intra-node) or TCP (inter-node). iOS nodes run services as in-process Swift actor threads — no sockets within the app.

```
  Client
     │ TCP
  ┌──▼──────────────────────────────────────────────────────────────────────┐
  │                   Load Balancer  (round-robin)                          │
  └──┬──────────────────────────────────────────────────────┬───────────────┘
     │ direct (in-process)                                  │ Unix socket / TCP
  ┌──▼────────────────────────────────┐      ┌──────────────▼─────────────────────────┐
  │  iOS Node                         │      │  Server Node (macOS/Linux)             │
  │                                   │ TCP  │   ┌── Unix ──┐    ┌──── TCP ────┐      │
  │  GreeterActor  AnalyticsActor     │◄────►│   └─────┬────┘    └──────┬──────┘      │
  │  InProcessServiceProvider         │      │         └───────┬────────┘             │
  │  ServiceClient                    │      │         Service Provider               │
  └───────────────┬───────────────────┘      │         Actor A ◄──► Actor B    ...   │
                  │                          │              (direct call)             │
                  │                          └─────────────────────────────────┬──────┘
                  │     SWIM gossip (swift-distributed-actors)                 │
                  └────────────────────────────────────────────────────────────┘
                                            │
                               ┌────────────▼────────────┐
                               │     Service Registry    │
                               │    (distributed actor)  │
                               └─────────────────────────┘
```

---

## Service discovery

The client never hardcodes addresses. Location is resolved at call time through the registry.

**Registration** — at provider startup:
```
ServiceProvider.host(service: GreeterService(), endpoint: .tcp(host:port:), registry:)
  → registers { name: "greeter", version: "1.0", endpoint: .tcp(...) } in ServiceRegistry
```

**Discovery** — at call time:
```
ServiceClient.call(serviceName: "greeter", ...)
  → queries ServiceRegistry for all live endpoints named "greeter"
  → LoadBalancer picks one (round-robin)
  → connects and sends Avro IPC request
```

`ServiceRegistry` is a distributed actor addressable cluster-wide — any node can query it. `HealthMonitor` watches SWIM membership events and deregisters endpoints belonging to dead nodes, so the registry stays accurate without manual cleanup.

---

## Request routing

When a service calls another service, the path depends on where the target lives:

| Target location | Transport | Serialization |
|---|---|---|
| Same `ServiceProvider` process | Direct actor call | None |
| Different process, same host | Unix domain socket | Avro |
| Different host | TCP | Avro |

The `ServiceProvider` checks whether the requested service is a local actor first. If yes, it dispatches directly — no socket, no serialization. Otherwise it looks up the endpoint in `ServiceRegistry` and forwards over Unix or TCP.

---

## Roles

| Role | Responsibility |
|---|---|
| **Service register / discover** | Tracks live provider endpoints; answers discovery queries |
| **Service provider** | Hosts services as actors; starts Avro IPC servers (server) or routes in-process (iOS) |
| **Client** | Discovers services and calls them via Avro IPC |
| **Cluster node / member** | Participates in SWIM cluster membership — every process is a member |
| **Health monitor** | Watches SWIM events; deregisters endpoints of dead nodes |
| **Load balancer / router** | Selects which provider instance handles each request |

---

## Transport

| Endpoint | Transport | Nodes |
|---|---|---|
| `Endpoint.inProcess(id:)` | Direct actor call — no socket | iOS / single-process node |
| `Endpoint.unix(path:)` | Unix domain socket | Intra-node on macOS / Linux |
| `Endpoint.tcp(host:port:)` | TCP | Inter-node (any platform) |

TCP and Unix share the same SwiftNIO pipeline and Avro codec — only the address differs.
In-process calls bypass all serialization for intra-app dispatch.

---

## Requirements

- Swift 6.0+
- macOS 15+ / Linux — full feature set
- iOS 18+ — TCP and in-process transports only (Unix sockets are sandbox-restricted)

---

## Package setup

```swift
// Package.swift
dependencies: [
    .package(path: "../SwiftAvroRpc"),
    .package(url: "https://github.com/apple/swift-distributed-actors.git", branch: "main"),
],
targets: [
    .target(
        name: "MyService",
        dependencies: [
            "SwiftAvroActor",
            .product(name: "DistributedCluster", package: "swift-distributed-actors"),
        ],
        resources: [.process("Resources")]  // required for Bundle.module / .avpr files
    )
]
```

---

## Defining a service

Every service has three parts: an Avro protocol file, a request handler, and a Swift conformance.

### 1. Avro protocol file

`Sources/MyService/Resources/greeter.avpr`

```json
{
  "protocol": "Greeter",
  "namespace": "com.example",
  "messages": {
    "hello": {
      "request": [{ "name": "name", "type": "string" }],
      "response": "string"
    }
  }
}
```

### 2. Request handler

```swift
import SwiftAvroRpc
import SwiftAvroCore

struct GreeterHandler: AvroIPCHandler {
    func handle(messageName: String, requestData: Data) async throws -> Data {
        let avro = Avro()
        let name: String = try avro.decode(from: requestData)
        return try avro.encode("Hello, \(name)!")
    }
}
```

### 3. AvroService conformance

```swift
import SwiftAvroActor

struct GreeterService: AvroService {
    var avroProtocol: String
    var serviceName:    String { "greeter" }
    var serviceVersion: String { "1.0.0" }
    var handler: any AvroIPCHandler { GreeterHandler() }

    init() throws {
        // .module refers to MyService's own bundle — the one that contains greeter.avpr
        self.avroProtocol = try AvroServiceDescriptor(resource: "greeter", in: .module).json
    }
}
```

---

## Server node (macOS / Linux)

```swift
import SwiftAvroActor
import DistributedCluster

// 1. Start the cluster (SWIM membership)
let node = try await ClusterNode(host: "10.0.0.1", port: 7337)

// 2. Create the shared service registry
let registry = ServiceRegistry(actorSystem: node.actorSystem)

// 3. Watch for dead nodes in the background
Task { await HealthMonitor(system: node.actorSystem, registry: registry).watch() }

// 4. Host services
let provider = ServiceProvider(nodeID: node.nodeID)

// TCP — reachable from any node
try await provider.host(
    service:  try GreeterService(),
    endpoint: .tcp(host: "10.0.0.1", port: 9090),
    registry: registry
)

// Unix socket — intra-node only, same host, lower latency
try await provider.host(
    service:  try AnalyticsService(),
    endpoint: .unix(path: "/var/run/myapp/analytics.sock"),
    registry: registry
)

// 5. Join the cluster (omit on the first seed node)
node.join(seedHost: "10.0.0.2", seedPort: 7337)
try await node.waitUntilUp()
```

---

## iOS node

```swift
import SwiftAvroActor
import DistributedCluster

// 1. Join the cluster over TCP
let node     = try await ClusterNode(host: deviceIP, port: 7337)
let registry = ServiceRegistry(actorSystem: node.actorSystem)
Task { await HealthMonitor(system: node.actorSystem, registry: registry).watch() }
node.join(seedHost: serverIP, seedPort: 7337)

// 2. Host services as in-process actors — no sockets
let local = InProcessServiceProvider(nodeID: node.nodeID)
try await local.host(service: try GreeterService(),   registry: registry)
try await local.host(service: try AnalyticsService(), registry: registry)

// 3. Call another in-process service directly (no network hop)
let responseData = try await local.callRaw(
    serviceName: "analytics",
    messageName: "track",
    requestData: encodedEvent
)

// 4. Call a remote server service over TCP
let client = ServiceClient(registry: registry)
let greeting: String = try await client.call(
    serviceName:    "greeter",
    clientProtocol: greeterProtocolJSON,
    messageName:    "hello",
    parameters:     ["World"],
    as:             String.self
)
```

---

## Calling a service (any node)

```swift
let client = ServiceClient(registry: registry)

let greeting: String = try await client.call(
    serviceName:    "greeter",
    clientProtocol: greeterProtocolJSON,
    messageName:    "hello",
    parameters:     ["World"],
    as:             String.self
)
```

`ServiceClient` handles endpoint selection (via `LoadBalancer`) and connection pooling. Pass `.tcp` or `.unix` endpoints — `.inProcess` endpoints must be called via `InProcessServiceProvider.callRaw` instead.

---

## Transport abstraction

SwiftAvroRpc defines transport as a protocol so call sites supply the address type:

```swift
public protocol AvroIPCServerTransport: Sendable {
    func bind(using bootstrap: ServerBootstrap) async throws -> any Channel
}
public protocol AvroIPCClientTransport: Sendable {
    func connect(using bootstrap: ClientBootstrap) async throws -> any Channel
}
```

| Type | Package | Endpoint |
|---|---|---|
| `TCPTransport` | SwiftAvroRpc | `.tcp` — inter-node |
| `UnixDomainTransport` | SwiftAvroActor | `.unix` — intra-node (macOS/Linux) |

---

## Project structure

```
Sources/SwiftAvroActor/
  Core/
    Endpoint.swift                — .tcp / .unix / .inProcess address enum
    ServiceInfo.swift             — registered service metadata (Codable)
    AvroService.swift             — protocol: avroProtocol + handler + name/version
    AvroServiceDescriptor.swift   — loads .avpr from a named bundle resource
  ClusterNode/
    ClusterNode.swift             — wraps ClusterSystem (SWIM membership)
  Registry/
    ServiceRegistry.swift         — distributed actor: register / deregister / discover
  Provider/
    ServiceProvider.swift         — hosts services, binds TCP or Unix sockets (macOS/Linux)
    InProcessServiceProvider.swift — hosts services as in-process actors (iOS)
    UnixDomainTransport.swift     — AvroIPCServerTransport / ClientTransport for Unix sockets
  HealthMonitor/
    HealthMonitor.swift           — watches cluster events, removes dead nodes from registry
  LoadBalancer/
    LoadBalancer.swift            — round-robin endpoint selection
  Client/
    ServiceClient.swift           — discovers service, selects endpoint, calls via Avro IPC
```

---

## License

Apache 2.0
