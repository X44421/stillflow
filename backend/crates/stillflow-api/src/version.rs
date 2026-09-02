//! API version negotiation primitives shared by all API transports.

use serde::{Deserialize, Serialize};

/// The only application API version currently exposed by Stillflow.
pub const API_V1: ApiVersion = ApiVersion(1);

/// Compile-time list used by handshake responses and manifest generation.
pub const SUPPORTED_API_VERSIONS: &[ApiVersion] = &[API_V1];

/// A wire-level application API version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ApiVersion(u16);

impl ApiVersion {
    pub const fn new(value: u16) -> Self {
        Self(value)
    }

    pub const fn value(self) -> u16 {
        self.0
    }

    pub fn is_supported(self) -> bool {
        SUPPORTED_API_VERSIONS.contains(&self)
    }
}

impl Default for ApiVersion {
    fn default() -> Self {
        API_V1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_is_transparent_on_the_wire() {
        assert_eq!(serde_json::to_string(&API_V1).expect("serialize"), "1");
        assert_eq!(
            serde_json::from_str::<ApiVersion>("1").expect("deserialize"),
            API_V1
        );
    }
}
