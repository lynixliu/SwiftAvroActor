import Foundation
import SwiftAvroCore
import SwiftAvroRpc

// MARK: - Wire type

/// Flat Avro-encodable cursor position update.
public struct CursorUpdate: Codable, Sendable, Equatable {
    public let blockId: String
    public let actorId: String
    public init(blockId: String, actorId: String) {
        self.blockId = blockId
        self.actorId = actorId
    }
}

// MARK: - Protocol definition

public enum CursorPresenceProtocol {

    /// Avro IPC protocol JSON for the cursor-presence service.
    public static let json: String = """
    {
      "protocol": "CursorPresenceProtocol",
      "namespace": "com.astropress.presence",
      "types": [
        {
          "type": "record",
          "name": "CursorUpdate",
          "fields": [
            {"name": "blockId", "type": "string"},
            {"name": "actorId", "type": "string"}
          ]
        }
      ],
      "messages": {
        "updateCursor": {
          "request": [{"name": "update", "type": "CursorUpdate"}],
          "one-way": true
        }
      }
    }
    """

    static let cursorUpdateSchema: AvroSchema = {
        Avro().newSchema(schema: """
        {"type":"record","name":"CursorUpdate",
         "namespace":"com.astropress.presence",
         "fields":[
           {"name":"blockId","type":"string"},
           {"name":"actorId","type":"string"}
         ]}
        """)!
    }()
}

// MARK: - Handler

struct CursorPresenceHandler: AvroIPCHandler, Sendable {
    let continuation: AsyncStream<CursorUpdate>.Continuation

    func handle(messageName: String, requestData: Data) async throws -> Data {
        guard messageName == "updateCursor" else { return Data() }
        let update: CursorUpdate = try Avro().decodeFrom(
            from: requestData, schema: CursorPresenceProtocol.cursorUpdateSchema
        )
        continuation.yield(update)
        return Data()
    }
}

// MARK: - Service

/// Avro IPC service that broadcasts cursor positions across cluster nodes.
///
/// Peers call `updateCursor` (one-way); each update lands in ``receivedUpdates``.
/// The application (e.g. `PresenceRegistry`) consumes the stream and updates its
/// block-level peer cursors.
public final class CursorPresenceService: AvroService, Sendable {
    public let avroProtocol:   String = CursorPresenceProtocol.json
    public let serviceName:    String = "cursor-presence"
    public let serviceVersion: String = "1.0.0"

    /// Yields each cursor update pushed by a remote peer.
    public let receivedUpdates: AsyncStream<CursorUpdate>

    private let _continuation: AsyncStream<CursorUpdate>.Continuation

    public var handler: any AvroIPCHandler {
        CursorPresenceHandler(continuation: _continuation)
    }

    public init() {
        let made    = AsyncStream<CursorUpdate>.makeStream()
        receivedUpdates = made.stream
        _continuation   = made.continuation
    }

    /// Terminates the `receivedUpdates` stream. Call on shutdown.
    public func finish() { _continuation.finish() }
}
