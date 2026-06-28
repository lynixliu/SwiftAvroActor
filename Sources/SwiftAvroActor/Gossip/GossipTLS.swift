import SwiftAvroRpc

/// TLS settings for cluster gossip traffic.
///
/// A node both *accepts* inbound gossip (acting as a TLS server) and *initiates*
/// outbound gossip and anti-entropy syncs (acting as a TLS client), so it needs a
/// distinct ``AvroTLSConfig`` for each role:
///
/// - `server` — the node's certificate chain and private key, presented to peers
///   that connect to its gossip server.
/// - `client` — the trust configuration used when connecting to peers.
///
/// Pass a `GossipTLS` to ``GossipRelay`` and ``GossipCatalog`` to encrypt all gossip
/// traffic; leave it `nil` (the default) for plain TCP.
public struct GossipTLS: Sendable {

    /// Server-side TLS (certificate + key) for the inbound gossip server.
    public let server: AvroTLSConfig

    /// Client-side TLS for outbound gossip and anti-entropy connections.
    public let client: AvroTLSConfig

    public init(server: AvroTLSConfig, client: AvroTLSConfig) {
        self.server = server
        self.client = client
    }

    /// Convenience: server identity from a PEM certificate/key pair, client using the
    /// system trust store.
    ///
    /// This is **one-way TLS** — peers authenticate this node's server certificate, but
    /// this node does not authenticate connecting peers. For a closed cluster use
    /// ``mutual(certificateFile:privateKeyFile:trustRootsFile:)`` instead.
    ///
    /// - Parameters:
    ///   - certificateFile: Path to the PEM certificate chain file.
    ///   - privateKeyFile:  Path to the PEM private key file.
    public static func pem(
        certificateFile: String,
        privateKeyFile:  String
    ) throws -> GossipTLS {
        GossipTLS(
            server: try .server(certificateFile: certificateFile, privateKeyFile: privateKeyFile),
            client: try .client()
        )
    }

    /// Convenience: **mutual TLS** for a private-CA cluster.
    ///
    /// Both the inbound server and the outbound client present this node's certificate and
    /// verify the peer's certificate against `trustRootsFile` (the shared CA, typically the
    /// cluster leader's CA). A peer without a CA-signed certificate is rejected at the
    /// handshake, in both directions.
    ///
    /// - Parameters:
    ///   - certificateFile: Path to this node's PEM certificate chain.
    ///   - privateKeyFile:  Path to this node's PEM private key.
    ///   - trustRootsFile:  Path to the PEM CA certificate(s) used to verify peers.
    public static func mutual(
        certificateFile: String,
        privateKeyFile:  String,
        trustRootsFile:  String
    ) throws -> GossipTLS {
        GossipTLS(
            server: try .mutualServer(
                certificateFile: certificateFile,
                privateKeyFile:  privateKeyFile,
                trustRootsFile:  trustRootsFile
            ),
            client: try .mutualClient(
                certificateFile: certificateFile,
                privateKeyFile:  privateKeyFile,
                trustRootsFile:  trustRootsFile
            )
        )
    }
}
