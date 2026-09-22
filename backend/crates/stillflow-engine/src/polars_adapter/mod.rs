//! Engine-side Polars compatibility adapter (P55-A2).
//!
//! Behaviour that changed between Polars 0.46 and 0.55 and is not expressible
//! through the domain types lives here, so the execution code keeps calling
//! StillFlow-owned helpers.

/// Runs a Polars call that may block in place from any Tokio runtime flavour.
///
/// Polars 0.55 lowers a `LazyFrame` into its physical plan through
/// `polars_async::RuntimeManager::block_in_place_on`, which calls
/// `tokio::task::block_in_place`. Tokio rejects that call when the ambient
/// runtime is single-threaded ("can call blocking only when running on the
/// multi-threaded runtime"), so every `collect()` inside a `#[tokio::test]` or
/// any current-thread embedder would panic where 0.46 simply ran the query.
///
/// This helper keeps the engine independent of the caller's runtime flavour: on
/// a current-thread runtime the closure runs on a scoped plain thread, which has
/// no runtime context, so Polars executes the call directly. On a multi-threaded
/// runtime — the production server runtime — the closure runs inline exactly as
/// before, so no additional thread hop enters the hot path.
pub(crate) fn blocking<R: Send>(f: impl FnOnce() -> R + Send) -> R {
    if on_current_thread_runtime() {
        match std::thread::scope(|scope| scope.spawn(f).join()) {
            Ok(value) => value,
            Err(payload) => std::panic::resume_unwind(payload),
        }
    } else {
        f()
    }
}

fn on_current_thread_runtime() -> bool {
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        // No runtime context: Polars runs the call directly.
        return false;
    };
    matches!(
        handle.runtime_flavor(),
        tokio::runtime::RuntimeFlavor::CurrentThread
    )
}
