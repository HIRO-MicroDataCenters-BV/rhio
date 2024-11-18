use anyhow::Result;
use async_nats::{HeaderMap, Message as NatsMessage};
use p2panda_core::{PrivateKey, PublicKey, Signature};

use crate::NetworkMessage;

/// Custom NATS header used by rhio to indicate that message was authored by this public key.
pub const NATS_RHIO_PUBLIC_KEY: &str = "X-Rhio-PublicKey";

/// Custom NATS header used by rhio to cryptographically authenticate this message.
pub const NATS_RHIO_SIGNATURE: &str = "X-Rhio-Signature";

pub fn has_nats_signature(headers: &Option<HeaderMap>) -> bool {
    if let Some(headers) = &headers {
        if headers.get(NATS_RHIO_SIGNATURE).is_some() || headers.get(NATS_RHIO_PUBLIC_KEY).is_some()
        {
            return true;
        }
    }
    false
}

/// This method checks if the public key given in the NATS header matches.
///
/// Important: This method does _not_ do any integrity or cryptography checks. We assume that this
/// method is used in contexts _after_ all validation took already place.
pub fn is_public_key_eq(message: &NatsMessage, expected_public_key: &PublicKey) -> bool {
    if let Some(headers) = &message.headers {
        let Some(_) = headers.get(NATS_RHIO_SIGNATURE) else {
            return false;
        };

        let Some(given_public_key) = headers.get(NATS_RHIO_PUBLIC_KEY) else {
            return false;
        };

        return given_public_key.to_string() == expected_public_key.to_string();
    }

    false
}

/// Add signature and public key as custom rhio NATS headers to message if they do not exist yet.
pub fn add_custom_nats_headers(
    previous_header: &Option<HeaderMap>,
    signature: Signature,
    public_key: PublicKey,
) -> HeaderMap {
    if has_nats_signature(previous_header) {
        return previous_header
            .to_owned()
            .expect("at this point we know there's a header");
    }

    let mut headers = match &previous_header {
        Some(headers) => headers.clone(),
        None => HeaderMap::new(),
    };
    headers.insert(NATS_RHIO_SIGNATURE, signature.to_string());
    headers.insert(NATS_RHIO_PUBLIC_KEY, public_key.to_string());
    headers
}

/// Remove potentially existing custom rhio NATS headers from a message.
pub fn remove_custom_nats_headers(previous_header: &HeaderMap) -> Option<HeaderMap> {
    let mut header = HeaderMap::new();
    for (key, values) in previous_header.iter() {
        if key.to_string() != NATS_RHIO_PUBLIC_KEY && key.to_string() != NATS_RHIO_SIGNATURE {
            for value in values {
                header.append(key.to_owned(), value.to_owned());
            }
        }
    }

    if header.len() == 0 {
        None
    } else {
        Some(header)
    }
}

/// Helper method automatically signing and wrapping NATS messages in a network message in case
/// they are not yet signed. Already signed messages (probably from other authors) just get
/// wrapped.
pub fn wrap_and_sign_nats_message(
    message: NatsMessage,
    private_key: &PrivateKey,
) -> Result<NetworkMessage> {
    // Wrap already signed NATS messages in a network message.
    if has_nats_signature(&message.headers) {
        return Ok(NetworkMessage::new_signed_nats(message)?);
    }

    // Sign them with our private key in case they are not.
    let mut network_message = NetworkMessage::new_nats(message, &private_key.public_key());
    network_message.sign(private_key);
    Ok(network_message)
}
