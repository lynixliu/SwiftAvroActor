import Foundation
import SwiftAvroCore
import SwiftAvroRpc

// MARK: - Protocol definition

/// Namespace for gossip wire-protocol constants.
enum GossipProtocol {

    /// Avro IPC protocol JSON used by both the gossip server and relay client.
    ///
    /// `propagate` is one-way; `sync` is two-way (request/response anti-entropy exchange).
    static let json = """
    {
      "protocol": "GossipProtocol",
      "namespace": "com.swiftavro.cluster",
      "types": [
        {
          "type": "record",
          "name": "ServiceInfoWire",
          "fields": [
            {"name": "name",         "type": "string"},
            {"name": "version",      "type": "string"},
            {"name": "nodeID",       "type": "string"},
            {"name": "endpointType", "type": "string"},
            {"name": "endpointHost", "type": "string"},
            {"name": "endpointPort", "type": "long"},
            {"name": "endpointPath", "type": "string"}
          ]
        },
        {
          "type": "record",
          "name": "DigestEntry",
          "fields": [
            {"name": "name",    "type": "string"},
            {"name": "nodeID",  "type": "string"},
            {"name": "version", "type": "string"}
          ]
        },
        {
          "type": "record",
          "name": "SyncRequest",
          "fields": [
            {"name": "digest", "type": {"type": "array", "items": "DigestEntry"}}
          ]
        },
        {
          "type": "record",
          "name": "SyncResponse",
          "fields": [
            {"name": "entries", "type": {"type": "array", "items": "ServiceInfoWire"}}
          ]
        }
      ],
      "messages": {
        "propagate": {
          "request": [{"name": "info", "type": "ServiceInfoWire"}],
          "one-way": true
        },
        "sync": {
          "request":  [{"name": "request", "type": "SyncRequest"}],
          "response": "SyncResponse"
        }
      }
    }
    """

    /// Standalone schema for `ServiceInfoWire` — used to decode `propagate` parameters.
    static let wireAvroSchema: AvroSchema = {
        Avro().newSchema(schema: """
        {
          "type": "record",
          "name": "ServiceInfoWire",
          "namespace": "com.swiftavro.cluster",
          "fields": [
            {"name": "name",         "type": "string"},
            {"name": "version",      "type": "string"},
            {"name": "nodeID",       "type": "string"},
            {"name": "endpointType", "type": "string"},
            {"name": "endpointHost", "type": "string"},
            {"name": "endpointPort", "type": "long"},
            {"name": "endpointPath", "type": "string"}
          ]
        }
        """)!
    }()

    /// Standalone schema for `SyncRequest` — used to decode incoming `sync` parameters.
    static let syncRequestSchema: AvroSchema = {
        Avro().newSchema(schema: """
        {
          "type": "record",
          "name": "SyncRequest",
          "namespace": "com.swiftavro.cluster",
          "fields": [
            {"name": "digest", "type": {"type": "array", "items": {
              "type": "record",
              "name": "DigestEntry",
              "fields": [
                {"name": "name",    "type": "string"},
                {"name": "nodeID",  "type": "string"},
                {"name": "version", "type": "string"}
              ]
            }}}
          ]
        }
        """)!
    }()

    /// Standalone schema for `SyncResponse` — used to encode the `sync` response.
    static let syncResponseSchema: AvroSchema = {
        Avro().newSchema(schema: """
        {
          "type": "record",
          "name": "SyncResponse",
          "namespace": "com.swiftavro.cluster",
          "fields": [
            {"name": "entries", "type": {"type": "array", "items": {
              "type": "record",
              "name": "ServiceInfoWire",
              "fields": [
                {"name": "name",         "type": "string"},
                {"name": "version",      "type": "string"},
                {"name": "nodeID",       "type": "string"},
                {"name": "endpointType", "type": "string"},
                {"name": "endpointHost", "type": "string"},
                {"name": "endpointPort", "type": "long"},
                {"name": "endpointPath", "type": "string"}
              ]
            }}}
          ]
        }
        """)!
    }()
}

// MARK: - Wire type

/// Flat, Avro-encodable representation of ``ServiceInfo``.
///
/// ``Endpoint`` is an enum with associated values and cannot be directly encoded as an
/// Avro record, so it is flattened into three fields:
///
/// | `endpointType` | `endpointHost` | `endpointPort` | `endpointPath` |
/// |---|---|---|---|
/// | `"tcp"`        | host           | port           | `""`           |
/// | `"unix"`       | `""`           | 0              | socket path    |
/// | `"inprocess"`  | `""`           | 0              | actor ID       |
struct ServiceInfoWire: Codable, Sendable {
    let name:         String
    let version:      String
    let nodeID:       String
    let endpointType: String
    let endpointHost: String
    let endpointPort: Int
    let endpointPath: String

    init(from info: ServiceInfo) {
        self.name    = info.name
        self.version = info.version
        self.nodeID  = info.nodeID
        switch info.endpoint {
        case .tcp(let host, let port):
            (endpointType, endpointHost, endpointPort, endpointPath) = ("tcp",       host, port, "")
        case .unix(let path):
            (endpointType, endpointHost, endpointPort, endpointPath) = ("unix",      "",   0,    path)
        case .inProcess(let id):
            (endpointType, endpointHost, endpointPort, endpointPath) = ("inprocess", "",   0,    id)
        }
    }

    func toServiceInfo() -> ServiceInfo {
        let endpoint: Endpoint = switch endpointType {
        case "tcp":  .tcp(host: endpointHost, port: endpointPort)
        case "unix": .unix(path: endpointPath)
        default:     .inProcess(id: endpointPath)
        }
        return ServiceInfo(name: name, version: version, endpoint: endpoint, nodeID: nodeID)
    }
}

// MARK: - Anti-entropy wire types

/// Minimal fingerprint for a single (service, node) pair — sent by the initiator of a sync round.
struct DigestEntry: Codable, Sendable, Hashable {
    let name:    String
    let nodeID:  String
    let version: String
}

/// Payload sent by the sync initiator: the set of (name, nodeID, version) triples it already holds.
struct SyncRequest: Codable, Sendable {
    let digest: [DigestEntry]
}

/// Payload returned by the sync responder: entries the initiator was missing.
struct SyncResponse: Codable, Sendable {
    let entries: [ServiceInfoWire]
}

// MARK: - Server-side handler

/// Receives `propagate` one-way calls from peer nodes and writes directly into
/// the local ``ServiceRegistry`` — bypassing ``GossipCatalog`` to prevent re-relay loops.
struct GossipHandler: AvroIPCHandler, Sendable {

    let registry: ServiceRegistry

    // MARK: Basic overload (no schema evolution)

    func handle(messageName: String, requestData: Data) async throws -> Data {
        let avro = Avro()
        switch messageName {
        case "propagate":
            let wire: ServiceInfoWire = try avro.decodeFrom(
                from: requestData, schema: GossipProtocol.wireAvroSchema
            )
            await registry.register(wire.toServiceInfo())
            return Data()
        case "sync":
            return try await handleSync(avro: avro, requestData: requestData,
                                        requestSchema: GossipProtocol.syncRequestSchema)
        default:
            return Data()
        }
    }

    // MARK: Schema-evolution-aware overload

    func handle(
        messageName:   String,
        requestData:   Data,
        writerSchemas: [AvroSchema],
        readerSchemas: [AvroSchema]
    ) async throws -> Data {
        let avro = Avro()
        switch messageName {
        case "propagate":
            let wire: ServiceInfoWire
            if let writer = writerSchemas.first, let reader = readerSchemas.first {
                wire = try avro.decodeFrom(from: requestData, writerSchema: writer, readerSchema: reader)
            } else {
                wire = try avro.decodeFrom(from: requestData, schema: GossipProtocol.wireAvroSchema)
            }
            await registry.register(wire.toServiceInfo())
            return Data()
        case "sync":
            return try await handleSync(avro: avro, requestData: requestData,
                                        requestSchema: GossipProtocol.syncRequestSchema)
        default:
            return Data()
        }
    }

    // MARK: Sync helper

    private func handleSync(avro: Avro, requestData: Data, requestSchema: AvroSchema) async throws -> Data {
        let syncReq: SyncRequest = try avro.decodeFrom(from: requestData, schema: requestSchema)
        let known = Set(syncReq.digest)
        let all   = await registry.allEntries()
        let missing = all.filter { info in
            !known.contains(DigestEntry(name: info.name, nodeID: info.nodeID, version: info.version))
        }.map { ServiceInfoWire(from: $0) }
        let response = SyncResponse(entries: missing)
        return try avro.encodeFrom(response, schema: GossipProtocol.syncResponseSchema)
    }
}

// MARK: - Errors

public enum GossipError: Error, Sendable {
    case invalidSchema
    case unsupportedEndpoint
}
