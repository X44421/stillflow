use std::time::Duration;

use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

/// Deadline and cancellation controls propagated through connector calls.
#[derive(Debug, Clone, Default)]
pub struct RequestContext {
    cancellation: CancellationToken,
    deadline: Option<Instant>,
}

impl RequestContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_cancellation(cancellation: CancellationToken) -> Self {
        Self {
            cancellation,
            deadline: None,
        }
    }

    pub fn with_deadline(deadline: Instant) -> Self {
        Self {
            cancellation: CancellationToken::new(),
            deadline: Some(deadline),
        }
    }

    pub fn with_cancellation_and_deadline(
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Self {
        Self {
            cancellation,
            deadline: Some(deadline),
        }
    }

    pub fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    pub fn ensure_active(&self) -> crate::ConnectorResult<()> {
        if self.is_cancelled() {
            return Err(crate::ConnectorError::cancelled());
        }
        if let Some(deadline) = self.deadline {
            if Instant::now() >= deadline {
                return Err(crate::ConnectorError::timeout("request deadline exceeded"));
            }
        }
        Ok(())
    }

    pub fn remaining(&self) -> Option<Duration> {
        self.deadline
            .and_then(|deadline| deadline.checked_duration_since(Instant::now()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancelled_context_is_detected() {
        let token = CancellationToken::new();
        let context = RequestContext::with_cancellation(token.clone());
        token.cancel();
        assert!(context.is_cancelled());
        assert_eq!(
            context.ensure_active().expect_err("cancelled").category(),
            crate::ErrorCategory::Cancelled
        );
    }

    #[test]
    fn expired_deadline_is_detected() {
        let context = RequestContext::with_deadline(Instant::now());
        std::thread::sleep(Duration::from_millis(5));
        assert_eq!(
            context.ensure_active().expect_err("timed out").category(),
            crate::ErrorCategory::Timeout
        );
    }

    #[test]
    fn carries_cancellation_and_deadline_together() {
        let token = CancellationToken::new();
        let deadline = Instant::now() + Duration::from_secs(30);
        let context = RequestContext::with_cancellation_and_deadline(token.clone(), deadline);
        assert!(context.ensure_active().is_ok());
        assert_eq!(context.deadline(), Some(deadline));
        token.cancel();
        assert_eq!(
            context.ensure_active().expect_err("cancelled").category(),
            crate::ErrorCategory::Cancelled
        );
    }
}
