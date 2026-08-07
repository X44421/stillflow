use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use stillflow_core::{ColumnId, Expr, LogicalError, LogicalType, ScalarValue};
use thiserror::Error;

/// Behavior when a cast cannot represent an input value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CastFailurePolicy {
    Error,
    SetNull,
}

/// Severity attached to a logical validation rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ValidationSeverity {
    Warning,
    Error,
}

/// Closed, engine-independent cleaning rule language.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "camelCase")]
pub enum Rule {
    Rename {
        column: ColumnId,
        to: String,
    },
    Cast {
        column: ColumnId,
        data_type: LogicalType,
        on_failure: CastFailurePolicy,
    },
    Trim {
        column: ColumnId,
    },
    ReplaceLiteral {
        column: ColumnId,
        from: ScalarValue,
        to: ScalarValue,
    },
    FillNull {
        column: ColumnId,
        value: ScalarValue,
    },
    DropColumn {
        column: ColumnId,
    },
    DeriveColumn {
        id: ColumnId,
        name: String,
        data_type: LogicalType,
        nullable: bool,
        expression: Expr,
    },
    FilterRows {
        predicate: Expr,
    },
    Deduplicate {
        keys: Vec<ColumnId>,
    },
    Validate {
        predicate: Expr,
        severity: ValidationSeverity,
        message: String,
    },
}

impl Rule {
    pub fn validate(&self) -> Result<(), RuleError> {
        match self {
            Self::Rename { to, .. } if to.trim().is_empty() => Err(RuleError::EmptyName),
            Self::Cast { data_type, .. } => data_type.validate().map_err(RuleError::from),
            Self::ReplaceLiteral { from, to, .. } => {
                Expr::Literal(from.clone()).validate_shape()?;
                Expr::Literal(to.clone())
                    .validate_shape()
                    .map_err(RuleError::from)
            }
            Self::FillNull { value, .. } => Expr::Literal(value.clone())
                .validate_shape()
                .map_err(RuleError::from),
            Self::DeriveColumn {
                name,
                data_type,
                expression,
                ..
            } => {
                if name.trim().is_empty() {
                    return Err(RuleError::EmptyName);
                }
                data_type.validate()?;
                expression.validate_shape().map_err(RuleError::from)
            }
            Self::FilterRows { predicate } => predicate.validate_shape().map_err(RuleError::from),
            Self::Deduplicate { keys } => {
                if keys.is_empty() {
                    return Err(RuleError::EmptyDeduplicationKeys);
                }
                let mut unique = BTreeSet::new();
                for key in keys {
                    if !unique.insert(*key) {
                        return Err(RuleError::DuplicateDeduplicationKey(*key));
                    }
                }
                Ok(())
            }
            Self::Validate {
                predicate, message, ..
            } => {
                if message.trim().is_empty() {
                    return Err(RuleError::EmptyValidationMessage);
                }
                Expr::Literal(ScalarValue::Utf8(message.clone())).validate_shape()?;
                predicate.validate_shape().map_err(RuleError::from)
            }
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RuleError {
    #[error("rule name must not be empty")]
    EmptyName,
    #[error("deduplication rule must contain at least one key")]
    EmptyDeduplicationKeys,
    #[error("duplicate deduplication key {0}")]
    DuplicateDeduplicationKey(ColumnId),
    #[error("validation message must not be empty")]
    EmptyValidationMessage,
    #[error(transparent)]
    Logical(#[from] LogicalError),
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    fn id(value: u128) -> ColumnId {
        ColumnId::from_uuid(Uuid::from_u128(value))
    }

    #[test]
    fn rejects_empty_names_keys_and_messages() {
        assert!(Rule::Rename {
            column: id(1),
            to: " ".to_owned(),
        }
        .validate()
        .is_err());
        assert!(Rule::Deduplicate { keys: Vec::new() }.validate().is_err());
        assert!(Rule::Validate {
            predicate: Expr::Column(id(1)),
            severity: ValidationSeverity::Error,
            message: String::new(),
        }
        .validate()
        .is_err());
    }

    #[test]
    fn rule_roundtrips_without_engine_objects() {
        let rule = Rule::DeriveColumn {
            id: id(2),
            name: "derived".to_owned(),
            data_type: LogicalType::Int64,
            nullable: false,
            expression: Expr::Column(id(1)),
        };
        rule.validate().expect("valid rule");
        let json = serde_json::to_vec(&rule).expect("serialize");
        let restored: Rule = serde_json::from_slice(&json).expect("deserialize");
        assert_eq!(restored, rule);
    }
}
