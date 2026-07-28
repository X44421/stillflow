//! Ingestion execution and orchestration for Stillflow sessions.
//!
//! `stillflow-engine` runs ingestion work inside a session: it drives
//! connectors, keeps memory bounded by streaming record batches, registers
//! imported data as datasets and snapshots, and records auditable events.
//!
//! Execution behavior is intentionally not implemented yet; orchestration
//! arrives after connector implementations land.

/// Returns the name of this crate, as a smoke test for workspace wiring.
pub fn crate_name() -> &'static str {
    "stillflow-engine"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crate_name_is_stable() {
        assert_eq!(crate_name(), "stillflow-engine");
    }
}
