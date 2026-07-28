//! External API surface exposing the Stillflow ingestion engine.
//!
//! `stillflow-api` translates external requests into engine calls and
//! serializes engine results back out. It is the only crate external
//! clients are meant to talk to, and it owns no ingestion logic itself.
//!
//! API routes are intentionally not implemented yet; the HTTP surface
//! arrives once preview and import flows exist in the engine.

/// Returns the name of this crate, as a smoke test for workspace wiring.
pub fn crate_name() -> &'static str {
    "stillflow-api"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crate_name_is_stable() {
        assert_eq!(crate_name(), "stillflow-api");
    }
}
