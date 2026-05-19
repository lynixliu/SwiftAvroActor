import Foundation
import SwiftAvroCore
import SwiftAvroRpc

// MARK: - Wire types

/// Parameters sent by a client requesting a remote LaTeX compilation.
public struct CompileRequest: Codable, Sendable, Equatable {
    public let texPath:        String
    public let outputDir:      String
    /// NodeID of the node hosting the callback service (e.g. `"127.0.0.1:9000"`).
    public let callbackNodeId: String
    /// Callback service host — where the compile daemon sends `onEvent` calls.
    public let callbackHost:   String
    /// Callback service port.
    public let callbackPort:   Int

    public init(
        texPath:        String,
        outputDir:      String,
        callbackNodeId: String,
        callbackHost:   String,
        callbackPort:   Int
    ) {
        self.texPath        = texPath
        self.outputDir      = outputDir
        self.callbackNodeId = callbackNodeId
        self.callbackHost   = callbackHost
        self.callbackPort   = callbackPort
    }
}

/// A single event emitted during (or at the end of) a compilation run.
public struct CompileEventWire: Codable, Sendable, Equatable {
    /// Event kind: `"stdout"`, `"stderr"`, `"success"`, or `"error"`.
    public let kind:    String
    /// Log text for stdout/stderr lines; empty for success/error without a message.
    public let text:    String
    /// Non-empty only when `kind == "success"` — path to the generated PDF.
    public let pdfPath: String

    public init(kind: String, text: String, pdfPath: String = "") {
        self.kind    = kind
        self.text    = text
        self.pdfPath = pdfPath
    }
}

// MARK: - Protocol definitions

public enum DocumentCompileProtocol {

    public static let serviceJson: String = """
    {
      "protocol": "DocumentCompileProtocol",
      "namespace": "com.astropress.compile",
      "types": [
        {
          "type": "record",
          "name": "CompileRequest",
          "fields": [
            {"name": "texPath",        "type": "string"},
            {"name": "outputDir",      "type": "string"},
            {"name": "callbackNodeId", "type": "string"},
            {"name": "callbackHost",   "type": "string"},
            {"name": "callbackPort",   "type": "long"}
          ]
        }
      ],
      "messages": {
        "compile": {
          "request": [{"name": "request", "type": "CompileRequest"}],
          "one-way": true
        }
      }
    }
    """

    static let compileRequestSchema: AvroSchema = {
        Avro().newSchema(schema: """
        {"type":"record","name":"CompileRequest",
         "namespace":"com.astropress.compile",
         "fields":[
           {"name":"texPath",        "type":"string"},
           {"name":"outputDir",      "type":"string"},
           {"name":"callbackNodeId", "type":"string"},
           {"name":"callbackHost",   "type":"string"},
           {"name":"callbackPort",   "type":"long"}
         ]}
        """)!
    }()
}

public enum CompileCallbackProtocol {

    public static let json: String = """
    {
      "protocol": "CompileCallbackProtocol",
      "namespace": "com.astropress.compile",
      "types": [
        {
          "type": "record",
          "name": "CompileEventWire",
          "fields": [
            {"name": "kind",    "type": "string"},
            {"name": "text",    "type": "string"},
            {"name": "pdfPath", "type": "string"}
          ]
        }
      ],
      "messages": {
        "onEvent": {
          "request": [{"name": "event", "type": "CompileEventWire"}],
          "one-way": true
        }
      }
    }
    """

    static let compileEventSchema: AvroSchema = {
        Avro().newSchema(schema: """
        {"type":"record","name":"CompileEventWire",
         "namespace":"com.astropress.compile",
         "fields":[
           {"name":"kind",    "type":"string"},
           {"name":"text",    "type":"string"},
           {"name":"pdfPath", "type":"string"}
         ]}
        """)!
    }()
}

// MARK: - Compile request controller (holds mutable handler closure)

actor DocumentCompileController {
    var handler: (@Sendable (CompileRequest) async -> Void)?

    func set(_ fn: @Sendable @escaping (CompileRequest) async -> Void) {
        handler = fn
    }

    func receive(_ request: CompileRequest) async {
        await handler?(request)
    }
}

// MARK: - DocumentCompileService

/// Avro IPC service hosted by the compile daemon.
///
/// The daemon registers a handler via ``setCompileHandler(_:)`` that performs
/// the actual `latexmk` invocation and sends ``CompileEventWire`` back to the
/// caller's ``CompileCallbackService`` endpoint.
public final class DocumentCompileService: AvroService, Sendable {
    public let avroProtocol:   String = DocumentCompileProtocol.serviceJson
    public let serviceName:    String = "document-compile"
    public let serviceVersion: String = "1.0.0"

    private let _controller: DocumentCompileController

    public var handler: any AvroIPCHandler {
        DocumentCompileHandler(controller: _controller)
    }

    public init() {
        _controller = DocumentCompileController()
    }

    /// Registers the closure invoked for each incoming `compile` request.
    public func setCompileHandler(_ fn: @Sendable @escaping (CompileRequest) async -> Void) async {
        await _controller.set(fn)
    }
}

struct DocumentCompileHandler: AvroIPCHandler, Sendable {
    let controller: DocumentCompileController

    func handle(messageName: String, requestData: Data) async throws -> Data {
        guard messageName == "compile" else { return Data() }
        let req: CompileRequest = try Avro().decodeFrom(
            from: requestData, schema: DocumentCompileProtocol.compileRequestSchema
        )
        await controller.receive(req)
        return Data()
    }
}

// MARK: - CompileCallbackService

/// Avro IPC service hosted by the compile **client** (the Mac editor).
///
/// The compile daemon connects back to this endpoint and sends ``CompileEventWire``
/// messages (stdout, stderr, success, error) as the `latexmk` run progresses.
public final class CompileCallbackService: AvroService, Sendable {
    public let avroProtocol:   String = CompileCallbackProtocol.json
    public let serviceName:    String = "compile-callback"
    public let serviceVersion: String = "1.0.0"

    /// Yields each ``CompileEventWire`` sent by the remote compile daemon.
    public let receivedEvents: AsyncStream<CompileEventWire>

    private let _continuation: AsyncStream<CompileEventWire>.Continuation

    public var handler: any AvroIPCHandler {
        CompileCallbackHandler(continuation: _continuation)
    }

    public init() {
        let made   = AsyncStream<CompileEventWire>.makeStream()
        receivedEvents = made.stream
        _continuation  = made.continuation
    }

    /// Terminates the `receivedEvents` stream. Call on shutdown.
    public func finish() { _continuation.finish() }
}

struct CompileCallbackHandler: AvroIPCHandler, Sendable {
    let continuation: AsyncStream<CompileEventWire>.Continuation

    func handle(messageName: String, requestData: Data) async throws -> Data {
        guard messageName == "onEvent" else { return Data() }
        let event: CompileEventWire = try Avro().decodeFrom(
            from: requestData, schema: CompileCallbackProtocol.compileEventSchema
        )
        continuation.yield(event)
        return Data()
    }
}
