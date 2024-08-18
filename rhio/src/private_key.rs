// SPDX-License-Identifier: AGPL-3.0-or-later

use std::fs::File;
use std::io::{Read, Write};
#[cfg(target_os = "linux")]
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

use anyhow::Result;
use p2panda_core::identity::PrivateKey;

/// Returns a new instance of `PrivateKey` by either loading the private key from a path or generating
/// a new one and saving it in the file system.
pub fn generate_or_load_private_key(path: PathBuf) -> Result<PrivateKey> {
    let private_key = if path.is_file() {
        load_private_key_from_file(path)?
    } else {
        let private_key = PrivateKey::new();
        save_private_key_to_file(&private_key, path)?;
        private_key
    };

    Ok(private_key)
}

/// Returns a new instance of `PrivateKey` by generating a new private key which is not persisted on the
/// file system.
///
/// This method is useful to run nodes for testing purposes.
pub fn generate_ephemeral_private_key() -> PrivateKey {
    PrivateKey::new()
}

/// Saves human-readable (hex-encoded) private key string (ed25519) into a file at the given path.
///
/// This method automatically creates the required directories on that path and fixes the
/// permissions of the file (0600, read and write permissions only for the owner).
#[cfg(target_os = "linux")]
fn save_private_key_to_file(private_key: &PrivateKey, path: PathBuf) -> Result<()> {
    let mut file = File::create(&path)?;
    file.write_all(private_key.as_bytes())?;
    file.sync_all()?;

    // Set permission for sensitive information
    let mut permissions = file.metadata()?.permissions();
    permissions.set_mode(0o600);
    std::fs::set_permissions(path, permissions)?;

    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn save_private_key_to_file(private_key: &PrivateKey, path: PathBuf) -> Result<()> {
    let mut file = File::create(path)?;
    file.write_all(private_key.to_hex().as_bytes())?;
    file.sync_all()?;

    Ok(())
}

/// Loads a private key from a file at the given path and derives ed25519 private key from it.
///
/// The private key in the file needs to be represented as a hex-encoded string.
fn load_private_key_from_file(path: PathBuf) -> Result<PrivateKey> {
    let mut file = File::open(path)?;
    let mut private_key_hex = String::new();
    file.read_to_string(&mut private_key_hex)?;
    let private_key = PrivateKey::try_from(&hex::decode(&private_key_hex)?[..])?;
    Ok(private_key)
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::generate_or_load_private_key;

    #[test]
    fn saves_and_loads_private_key() {
        let tmp_dir = TempDir::new().unwrap();
        let mut tmp_path = tmp_dir.path().to_owned();
        tmp_path.push("private-key.txt");

        // Attempt to load the private key from the temporary path
        // This should result in a new private key being generated and written to file
        let private_key = generate_or_load_private_key(tmp_path.clone());
        assert!(private_key.is_ok(), "{:?}", private_key.err());

        // Attempt to load the private key from the same temporary path
        // This should result in the previously-generated private key being loaded from file
        let private_key_2 = generate_or_load_private_key(tmp_path);
        assert!(private_key_2.is_ok());

        // Ensure that both private keys have the same public key
        assert_eq!(
            private_key.unwrap().public_key(),
            private_key_2.unwrap().public_key()
        );
    }
}
