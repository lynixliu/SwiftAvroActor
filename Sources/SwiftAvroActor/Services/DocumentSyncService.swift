import Foundation
import SwiftAvroCore
import SwiftAvroRpc

// MARK: - Wire types

/// Flat Avro-encodable container for a JSON-serialised `DocumentEvent`.
public struct DocumentEventWire: Codable, Sendable, Equatable {
    public let eventJson: String
    public init(eventJson: String) { self.eventJson = eventJson }
}

/// Parameters for a `syncRange` RPC call.
public struct SyncRangeRequest: Codable, Sendable, Equatable {
    public let fromLamport: Int64
    public let toLamport: Int64
    public init(fromLamport: Int64, toLamport: Int64) {
        self.fromLamport = fromLamport
        self.toLamport   = toLamport
    }
}

/// Result of a `syncRange` RPC call — the events this node was missing.
public struct SyncRangeResponse: Codable, Sendable, Equatable {
    public let events: [DocumentEventWire]
    public init(events: [DocumentEventWire]) { self.events = events }
}

// MARK: - Protocol definition

public enum DocumentSyncProtocol {

    /// Avro IPC protocol JSON for the document-sync service.
    public static let json: String = """
    {
      "protocol": "DocumentSyncProtocol",
      "namespace": "com.astropress.sync",
      "types": [
        {
          "type": "record",
          "name": "DocumentEventWire",
          "fields": [{"name": "eventJson", "type": "string"}]
        },
        {
          "type": "record",
          "name": "SyncRangeRequest",
          "fields": [
            {"name": "fromLamport", "type": "long"},
            {"name": "toLamport",   "type": "long"}
          ]
        },
        {
          "type": "record",
          "name": "SyncRangeResponse",
          "fields": [
            {"name": "events",
             "type": {"type": "array", "items": "DocumentEventWire"}}
          ]
        }
      ],
      "messages": {
        "pushEvent": {
          "request":  [{"name": "event",   "type": "DocumentEventWire"}],
          "one-way":  true
        },
        "syncRange": {
          "request":  [{"name": "request", "type": "SyncRangeRequest"}],
          "response": "SyncRangeResponse"
        }
      }
    }
    """

    // MARK: Stand-alone schemas (handler decode / encode)

    static let eventWireSchema: AvroSchema = {
        Avro().newSchema(schema: """
        {"type":"record","name":"DocumentEventWire",
         "namespace":"com.astropress.sync",
         "fields":[{"name":"eventJson","type":"string"}]}
        """)!
    }()

    static let syncRangeRequestSchema: AvroSchema = {
        Avro().newSchema(schema: """
        {"type":"record","name":"SyncRangeRequest",
         "namespace":"com.astropress.sync",
         "fields":[
           {"name":"fromLamport","type":"long"},
           {"name":"toLamport","type":"long"}
         ]}
        """)!
    }()

    static let syncRangeResponseSchema: AvroSchema = {
        Avro().newSchema(schema: """
        {"type":"record","name":"SyncRangeResponse",
         "namespace":"com.astropress.sync",
         "fields":[{"name":"events","type":{"type":"array","items":{
           "type":"record","name":"DocumentEventWire",
           "fields":[{"name":"eventJson","type":"string"}]
         }}}]}
        """)!
    }()
}

// MARK: - Range provider actor

/// Holds the mutable closure used to answer `syncRange` requests from peers.
actor DocumentSyncProvider {
    var provider: (@Sendable (Int64, Int64) async -> [String])?

    func set(_ fn: @Sendable @escaping (Int64, Int64) async -> [String]) {
        provider = fn
    }

    func fetch(from: Int64, to: Int64) async -> [String] {
        await provider?(from, to) ?? []
    }
}

// MARK: - Handler

/// Server-side handler for incoming `pushEvent` and `syncRange` calls.
struct DocumentSyncHandler: AvroIPCHandler, Sendable {
    let continuation: AsyncStream<String>.Continuation
    let syncProvider: DocumentSyncProvider

    func handle(messageName: String, requestData: Data) async throws -> Data {
        switch messageName {
        case "pushEvent":
            let wire: DocumentEventWire = try Avro().decodeFrom(
                from: requestData, schema: DocumentSyncProtocol.eventWireSchema
            )
            continuation.yield(wire.eventJson)
            return Data()

        case "syncRange":
            let req: SyncRangeRequest = try Avro().decodeFrom(
                from: requestData, schema: DocumentSyncProtocol.syncRangeRequestSchema
            )
            let jsons = await syncProvider.fetch(from: req.fromLamport, to: req.toLamport)
            let resp  = SyncRangeResponse(events: jsons.map { DocumentEventWire(eventJson: $0) })
            return try Avro().encodeFrom(resp, schema: DocumentSyncProtocol.syncRangeResponseSchema)

        default:
            return Data()
        }
    }
}

// MARK: - Service

/// Avro IPC service that synchronises `DocumentEvent` JSON payloads between cluster nodes.
///
/// **Receive path** — peers call `pushEvent`; the JSON lands in ``receivedEvents``.
/// The application validates each event through the four-gate chain before appending
/// it to the local `EventLog`.
///
/// **Serve path** — peers call `syncRange` to fetch events this node holds.
/// Register a provider with ``setSyncProvider(_:)`` before hosting the service.
public final class DocumentSyncService: AvroService, Sendable {
    public let avroProtocol:   String = DocumentSyncProtocol.json
    public let serviceName:    String = "document-sync"
    public let serviceVersion: String = "1.0.0"

    /// Yields a JSON string for each event pushed by a remote peer.
    public let receivedEvents: AsyncStream<String>

    private let _continuation: AsyncStream<String>.Continuation
    private let _provider:     DocumentSyncProvider

    public var handler: any AvroIPCHandler {
        DocumentSyncHandler(continuation: _continuation, syncProvider: _provider)
    }

    public init() {
        let made    = AsyncStream<String>.makeStream()
        receivedEvents = made.stream
        _continuation  = made.continuation
        _provider      = DocumentSyncProvider()
    }

    /// Registers the closure invoked when a peer calls `syncRange`.
    public func setSyncProvider(_ fn: @Sendable @escaping (Int64, Int64) async -> [String]) async {
        await _provider.set(fn)
    }

    /// Terminates the `receivedEvents` stream. Call on shutdown.
    public func finish() { _continuation.finish() }
}
