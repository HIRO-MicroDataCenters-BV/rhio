/// Custom NATS header used by rhio to indicate that message was authored by this public key.
pub const NATS_RHIO_PUBLIC_KEY: &str = "X-Rhio-PublicKey";

/// Custom NATS header used by rhio to cryptographically authenticate this message.
pub const NATS_RHIO_SIGNATURE: &str = "X-Rhio-Signature";
