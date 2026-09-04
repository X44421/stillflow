//! Deterministic, secret-free schedule values shared by the automation
//! scheduler and its future API projection.

use std::str::FromStr;

use chrono::{DateTime, Duration, LocalResult, NaiveDate, NaiveTime, TimeZone, Utc};
use chrono_tz::Tz;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const AUTOMATION_SCHEDULE_VERSION: u16 = 1;
pub const MAX_SCHEDULE_PERIOD_SECONDS: u64 = 366 * 24 * 60 * 60;
pub const MAX_TIMEZONE_NAME_BYTES: usize = 64;
const MAX_DAILY_SEARCH_DAYS: u32 = 370;

/// The bounded v1 schedule set. Interval schedules use UTC elapsed time;
/// daily schedules use one local wall-clock occurrence per calendar day.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum AutomationSchedule {
    Interval { period_seconds: u64 },
    Daily { hour: u8, minute: u8, second: u8 },
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum AutomationScheduleError {
    #[error("schedule period is outside the supported bound")]
    InvalidPeriod,
    #[error("local time is outside the supported range")]
    InvalidLocalTime,
    #[error("timezone name is empty or too long")]
    InvalidTimezoneName,
    #[error("timezone is not a supported IANA timezone")]
    InvalidTimezone,
    #[error("no valid daily occurrence was found within the bounded horizon")]
    SearchHorizonExceeded,
    #[error("schedule timestamp arithmetic overflowed")]
    TimestampOverflow,
}

impl AutomationSchedule {
    pub fn validate(&self) -> Result<(), AutomationScheduleError> {
        match self {
            Self::Interval { period_seconds }
                if (1..=MAX_SCHEDULE_PERIOD_SECONDS).contains(period_seconds) =>
            {
                Ok(())
            }
            Self::Interval { .. } => Err(AutomationScheduleError::InvalidPeriod),
            Self::Daily {
                hour,
                minute,
                second,
            } if *hour < 24 && *minute < 60 && *second < 60 => Ok(()),
            Self::Daily { .. } => Err(AutomationScheduleError::InvalidLocalTime),
        }
    }

    pub fn validate_timezone(timezone: &str) -> Result<Tz, AutomationScheduleError> {
        if timezone.is_empty() || timezone.len() > MAX_TIMEZONE_NAME_BYTES {
            return Err(AutomationScheduleError::InvalidTimezoneName);
        }
        Tz::from_str(timezone).map_err(|_| AutomationScheduleError::InvalidTimezone)
    }

    /// Returns the first occurrence at or after `start`.
    pub fn first_at_or_after(
        &self,
        start: DateTime<Utc>,
        timezone: &str,
    ) -> Result<DateTime<Utc>, AutomationScheduleError> {
        self.validate()?;
        let tz = Self::validate_timezone(timezone)?;
        match self {
            Self::Interval { .. } => Ok(start),
            Self::Daily { .. } => {
                let local = start.with_timezone(&tz);
                let date = local.date_naive();
                self.search_daily_from(date, start, tz)
            }
        }
    }

    /// Returns the next strictly-later occurrence. For a fall-back ambiguous
    /// local time the earlier instant is selected, so one wall-clock event is
    /// never emitted twice. A spring-forward nonexistent local time is skipped
    /// and the next valid calendar day is selected.
    pub fn next_after(
        &self,
        previous: DateTime<Utc>,
        timezone: &str,
    ) -> Result<DateTime<Utc>, AutomationScheduleError> {
        self.validate()?;
        let tz = Self::validate_timezone(timezone)?;
        match self {
            Self::Interval { period_seconds } => previous
                .checked_add_signed(Duration::seconds(
                    i64::try_from(*period_seconds)
                        .map_err(|_| AutomationScheduleError::TimestampOverflow)?,
                ))
                .ok_or(AutomationScheduleError::TimestampOverflow),
            Self::Daily { .. } => {
                let local_date = previous.with_timezone(&tz).date_naive();
                let date = local_date
                    .succ_opt()
                    .ok_or(AutomationScheduleError::TimestampOverflow)?;
                self.search_daily_from(date, previous, tz)
            }
        }
    }

    fn search_daily_from(
        &self,
        mut date: NaiveDate,
        not_before: DateTime<Utc>,
        timezone: Tz,
    ) -> Result<DateTime<Utc>, AutomationScheduleError> {
        let Self::Daily {
            hour,
            minute,
            second,
        } = self
        else {
            return Err(AutomationScheduleError::InvalidLocalTime);
        };
        let local_time =
            NaiveTime::from_hms_opt(u32::from(*hour), u32::from(*minute), u32::from(*second))
                .ok_or(AutomationScheduleError::InvalidLocalTime)?;
        for _ in 0..MAX_DAILY_SEARCH_DAYS {
            let local = date.and_time(local_time);
            let candidate = match timezone.from_local_datetime(&local) {
                LocalResult::Single(value) => Some(value.with_timezone(&Utc)),
                LocalResult::Ambiguous(earlier, _) => Some(earlier.with_timezone(&Utc)),
                LocalResult::None => None,
            };
            if let Some(candidate) = candidate {
                if candidate >= not_before {
                    return Ok(candidate);
                }
            }
            date = date
                .succ_opt()
                .ok_or(AutomationScheduleError::TimestampOverflow)?;
        }
        Err(AutomationScheduleError::SearchHorizonExceeded)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utc(value: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(value)
            .expect("RFC3339 timestamp")
            .with_timezone(&Utc)
    }

    #[test]
    fn dst_forward_skips_gap_and_dst_backward_emits_one_earlier_instant() {
        let spring = AutomationSchedule::Daily {
            hour: 2,
            minute: 30,
            second: 0,
        };
        assert_eq!(
            spring
                .first_at_or_after(utc("2024-03-10T06:00:00Z"), "America/New_York")
                .expect("spring next run"),
            utc("2024-03-11T06:30:00Z")
        );

        let fall = AutomationSchedule::Daily {
            hour: 1,
            minute: 30,
            second: 0,
        };
        let first = fall
            .first_at_or_after(utc("2024-11-03T04:00:00Z"), "America/New_York")
            .expect("fall first run");
        assert_eq!(first, utc("2024-11-03T05:30:00Z"));
        assert_eq!(
            fall.next_after(first, "America/New_York")
                .expect("fall next run"),
            utc("2024-11-04T06:30:00Z")
        );
    }
}
