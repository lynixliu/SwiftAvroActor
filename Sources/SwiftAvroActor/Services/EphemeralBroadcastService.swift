import Foundation
import SwiftAvroCore
import SwiftAvroRpc

// MARK: - Wire type

/// Discriminated-union wire container for one-way ephemeral messages.
/// `type` is "bullet" or "readerPresence"; `json` carries the JSON-encoded payload.
public struct EphemeralMessage: Codable, Sendable, Equatable {
    public let type: String
    public let json: String
    public init(type: String, json: String) {
        self.type = type
        self.json = json
    }
}

// MARK: - Protocol definition

public enum EphemeralBroadcastProtocol {

    public static let json: String = """
    {
      "protocol": "EphemeralBroadcastProtocol",
      "namespace": "com.astropress.ephemeral",
      "types": [
        {
          "type": "record",
          "name": "EphemeralMessage",
          "fields": [
            {"name": "type", "type": "string"},
            {"name": "json", "type": "string"}
          ]
        }
      ],
      "messages": {
        "broadcast": {
          "request": [{"name": "msg", "type": "EphemeralMessage"}],
          "one-way": true
        }
      }
    }
    """

    static let messageSchema: AvroSchema = {
        Avro().newSchema(schema: """
        {"type":"record","name":"EphemeralMessage",
         "namespace":"com.astropress.ephemeral",
         "fields":[
           {"name":"type","type":"string"},
           {"name":"json","type":"string"}
         ]}
        """)!
    }()
}

// MARK: - Handler

struct EphemeralBroadcastHandler: AvroIPCHandler, Sendable {
    let continuation: AsyncStream<EphemeralMessage>.Continuation

    func handle(messageName: String, requestData: Data) async throws -> Data {
        guard messageName == "broadcast" else { return Data() }
        let msg: EphemeralMessage = try Avro().decodeFrom(
            from: requestData, schema: EphemeralBroadcastProtocol.messageSchema
        )
        continuation.yield(msg)
        return Data()
    }
}

// MARK: - Service

/// Avro IPC service for ephemeral SWIM broadcast of `BulletComment` and
/// `ReaderPresenceRecord`. Each peer calls `broadcast` (one-way); the payload
/// lands in ``receivedMessages`` as a typed `EphemeralMessage`.
///
/// Callers decode by inspecting `msg.type`:
/// - `"bullet"` → `JSONDecoder().decode(BulletComment.self, from: msg.json.data(…))`
/// - `"readerPresence"` → `JSONDecoder().decode(ReaderPresenceRecord.self, from: msg.json.data(…))`
public final class EphemeralBroadcastService: AvroService, Sendable {
    public let avroProtocol:   String = EphemeralBroadcastProtocol.json
    public let serviceName:    String = "ephemeral-broadcast"
    public let serviceVersion: String = "1.0.0"

    /// Yields each message broadcast by a remote peer.
    public let receivedMessages: AsyncStream<EphemeralMessage>
    private let _continuation:   AsyncStream<EphemeralMessage>.Continuation

    public var handler: any AvroIPCHandler {
        EphemeralBroadcastHandler(continuation: _continuation)
    }

    public init() {
        let made         = AsyncStream<EphemeralMessage>.makeStream()
        receivedMessages = made.stream
        _continuation    = made.continuation
    }

    public func finish() { _continuation.finish() }
}
