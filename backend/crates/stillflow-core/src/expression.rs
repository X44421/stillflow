use std::collections::BTreeSet;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{ensure_no_secret_fields, ColumnId, LogicalError, LogicalSchema, LogicalType};

/// Finite, canonicalized IEEE-754 value suitable for equality and stable JSON.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FiniteF64(u64);

impl FiniteF64 {
    pub fn new(value: f64) -> Result<Self, LogicalError> {
        if !value.is_finite() {
            return Err(LogicalError::NonFiniteFloat);
        }
        let canonical = if value == 0.0 { 0.0 } else { value };
        Ok(Self(canonical.to_bits()))
    }

    pub fn get(self) -> f64 {
        f64::from_bits(self.0)
    }
}

impl Serialize for FiniteF64 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_f64(self.get())
    }
}

impl<'de> Deserialize<'de> for FiniteF64 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = f64::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

/// Literal values admitted by the version 1 expression language.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "camelCase")]
pub enum ScalarValue {
    Null,
    Boolean(bool),
    Int64(i64),
    UInt64(u64),
    Float64(FiniteF64),
    Utf8(String),
}

impl ScalarValue {
    pub fn float64(value: f64) -> Result<Self, LogicalError> {
        FiniteF64::new(value).map(Self::Float64)
    }
}

/// Closed unary operators supported by logical expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum UnaryOperator {
    Not,
    Negate,
}

/// Closed binary operators supported by logical expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum BinaryOperator {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    And,
    Or,
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    Contains,
}

/// Engine-independent, serializable logical expression.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "camelCase")]
pub enum Expr {
    Column(ColumnId),
    Literal(ScalarValue),
    Unary {
        operator: UnaryOperator,
        expression: Box<Expr>,
    },
    Binary {
        left: Box<Expr>,
        operator: BinaryOperator,
        right: Box<Expr>,
    },
    IsNull {
        expression: Box<Expr>,
        negated: bool,
    },
    Cast {
        expression: Box<Expr>,
        data_type: LogicalType,
    },
    Coalesce {
        expressions: Vec<Expr>,
    },
}

impl Expr {
    /// Returns sorted, de-duplicated column references without recursive traversal.
    pub fn referenced_columns(&self) -> BTreeSet<ColumnId> {
        let mut columns = BTreeSet::new();
        let mut pending = vec![self];
        while let Some(expression) = pending.pop() {
            match expression {
                Self::Column(id) => {
                    columns.insert(*id);
                }
                Self::Literal(_) => {}
                Self::Unary { expression, .. }
                | Self::IsNull { expression, .. }
                | Self::Cast { expression, .. } => pending.push(expression),
                Self::Binary { left, right, .. } => {
                    pending.push(right);
                    pending.push(left);
                }
                Self::Coalesce { expressions } => pending.extend(expressions),
            }
        }
        columns
    }

    /// Validates expression-local invariants that do not require a schema.
    pub fn validate_shape(&self) -> Result<(), LogicalError> {
        let mut pending = vec![self];
        while let Some(expression) = pending.pop() {
            match expression {
                Self::Column(_) => {}
                Self::Literal(ScalarValue::Utf8(value)) => {
                    ensure_no_secret_fields(&serde_json::Value::String(value.clone()))
                        .map_err(|_| LogicalError::UnsafeLiteral)?;
                }
                Self::Literal(_) => {}
                Self::Unary { expression, .. } | Self::IsNull { expression, .. } => {
                    pending.push(expression);
                }
                Self::Cast {
                    expression,
                    data_type,
                } => {
                    data_type.validate()?;
                    pending.push(expression);
                }
                Self::Binary { left, right, .. } => {
                    pending.push(right);
                    pending.push(left);
                }
                Self::Coalesce { expressions } => {
                    if expressions.is_empty() {
                        return Err(LogicalError::EmptyCoalesce);
                    }
                    pending.extend(expressions);
                }
            }
        }
        Ok(())
    }

    /// Validates local shape and verifies every column reference against a schema.
    pub fn validate(&self, schema: &LogicalSchema) -> Result<(), LogicalError> {
        self.validate_shape()?;
        schema.validate()?;
        for id in self.referenced_columns() {
            if schema.field(id).is_none() {
                return Err(LogicalError::UnknownColumn(id));
            }
        }
        Ok(())
    }
}

/// Structurally typed connector predicate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceFilter {
    pub expression: Expr,
}

impl SourceFilter {
    pub fn new(expression: Expr) -> Result<Self, LogicalError> {
        expression.validate_shape()?;
        Ok(Self { expression })
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;
    use crate::LogicalField;

    fn id(value: u128) -> ColumnId {
        ColumnId::from_uuid(Uuid::from_u128(value))
    }

    #[test]
    fn float_literals_reject_non_finite_values_and_canonicalize_zero() {
        assert!(ScalarValue::float64(f64::NAN).is_err());
        assert!(ScalarValue::float64(f64::INFINITY).is_err());
        assert_eq!(
            ScalarValue::float64(-0.0).expect("finite"),
            ScalarValue::float64(0.0).expect("finite")
        );
    }

    #[test]
    fn references_are_sorted_and_deduplicated() {
        let expression = Expr::Binary {
            left: Box::new(Expr::Column(id(2))),
            operator: BinaryOperator::And,
            right: Box::new(Expr::Binary {
                left: Box::new(Expr::Column(id(1))),
                operator: BinaryOperator::Equal,
                right: Box::new(Expr::Column(id(2))),
            }),
        };
        assert_eq!(
            expression
                .referenced_columns()
                .into_iter()
                .collect::<Vec<_>>(),
            vec![id(1), id(2)]
        );
    }

    #[test]
    fn validates_references_against_logical_schema() {
        let schema = LogicalSchema::new(vec![LogicalField::new(
            id(1),
            "known",
            LogicalType::Int64,
            false,
        )
        .expect("field")])
        .expect("schema");
        Expr::Column(id(1)).validate(&schema).expect("known column");
        assert!(matches!(
            Expr::Column(id(2)).validate(&schema),
            Err(LogicalError::UnknownColumn(column)) if column == id(2)
        ));
    }

    #[test]
    fn rejects_empty_coalesce_and_secret_like_literals() {
        let empty = Expr::Coalesce {
            expressions: Vec::new(),
        };
        assert_eq!(empty.validate_shape(), Err(LogicalError::EmptyCoalesce));

        let unsafe_literal =
            Expr::Literal(ScalarValue::Utf8("token=must-not-enter-plan".to_owned()));
        assert_eq!(
            unsafe_literal.validate_shape(),
            Err(LogicalError::UnsafeLiteral)
        );
    }

    #[test]
    fn expression_json_is_deterministic_and_roundtrips() {
        let expression = Expr::Cast {
            expression: Box::new(Expr::Column(id(1))),
            data_type: LogicalType::Timestamp {
                unit: crate::TimeUnit::Microsecond,
                timezone: Some("UTC".to_owned()),
            },
        };
        let first = serde_json::to_vec(&expression).expect("serialize");
        let restored: Expr = serde_json::from_slice(&first).expect("deserialize");
        let second = serde_json::to_vec(&restored).expect("serialize again");
        assert_eq!(first, second);
    }
}
