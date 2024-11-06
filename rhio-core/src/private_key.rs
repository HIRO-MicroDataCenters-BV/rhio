use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

use anyhow::Result;
use p2panda_core::identity::PrivateKey;

/// Loads a private key from a file at the given path and derives ed25519 private key from it.
///
/// The private key in the file needs to be represented as a hex-encoded string.
pub fn load_private_key_from_file(path: PathBuf) -> Result<PrivateKey> {
    let mut file = File::open(path)?;
    let mut private_key_hex = String::new();
    file.read_to_string(&mut private_key_hex)?;
    let private_key = PrivateKey::try_from(&hex::decode(&private_key_hex)?[..])?;
    Ok(private_key)
}
