//! Connector contracts and registry boundary for Stillflow data sources.
//!
//! `stillflow-connectors` defines the single connector contract used for
//! discovery, inspection, preview, streaming reads and checkpoints, plus
//! the registry that maps source types to connector implementations.
//!
//! Connector behavior is intentionally not implemented yet; the contract
//! traits land with the Arrow connector interface milestone.

/// Returns the name of this crate, as a smoke test for workspace wiring.
pub fn crate_name() -> &'static str {
    "stillflow-connectors"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crate_name_is_stable() {
        assert_eq!(crate_name(), "stillflow-connectors");
    }
}
