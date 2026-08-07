use std::fmt;
use std::fs::File;
use std::io::Read;

use serde::{de::Error as DeError, Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};

use crate::StorageError;

pub const DIGEST_BUFFER_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContentDigest([u8; 32]);

impl ContentDigest {
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn try_from_hex(value: &str) -> Result<Self, StorageError> {
        if value.len() != 64 {
            return Err(StorageError::InvalidManifest("invalid SHA-256 length"));
        }

        let mut bytes = [0_u8; 32];
        for (target, pair) in bytes.iter_mut().zip(value.as_bytes().chunks_exact(2)) {
            let text = std::str::from_utf8(pair)
                .map_err(|_| StorageError::InvalidManifest("invalid SHA-256 encoding"))?;
            *target = u8::from_str_radix(text, 16)
                .map_err(|_| StorageError::InvalidManifest("invalid SHA-256 encoding"))?;
        }
        Ok(Self(bytes))
    }
}

impl fmt::Display for ContentDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl Serialize for ContentDigest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for ContentDigest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::try_from_hex(&value).map_err(DeError::custom)
    }
}

pub(crate) fn digest_file(file: &mut File) -> Result<ContentDigest, StorageError> {
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; DIGEST_BUFFER_BYTES];
    loop {
        let count = file
            .read(&mut buffer)
            .map_err(|error| StorageError::io("read partition checksum", &error))?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    Ok(ContentDigest(hasher.finalize().into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digest_hex_roundtrips() {
        let digest = ContentDigest([0xab; 32]);
        assert_eq!(
            ContentDigest::try_from_hex(&digest.to_string()).expect("valid digest"),
            digest
        );
        assert!(ContentDigest::try_from_hex("00").is_err());
        assert!(ContentDigest::try_from_hex(&"zz".repeat(32)).is_err());
    }
}
