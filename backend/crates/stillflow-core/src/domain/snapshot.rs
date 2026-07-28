use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::ConnectorResult;

/// Serializable snapshot of one Arrow schema field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaFieldSnapshot {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

impl SchemaFieldSnapshot {
    fn from_field(field: &Field) -> Self {
        Self {
            name: field.name().to_owned(),
            data_type: format!("{:?}", field.data_type()),
            nullable: field.is_nullable(),
        }
    }

    fn to_field(&self) -> Field {
        Field::new(self.name.clone(), DataType::Utf8, self.nullable)
    }
}

/// Immutable materialized output plus lineage and quality metadata.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DatasetSnapshot {
    pub id: Uuid,
    pub dataset_id: Uuid,
    pub session_id: Uuid,
    pub storage_ref: String,
    pub row_count: u64,
    pub quality_score: Option<u8>,
    pub lineage: Vec<Uuid>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub schema_fields: Vec<SchemaFieldSnapshot>,
    #[serde(skip)]
    pub schema: Option<Arc<Schema>>,
    pub created_at: DateTime<Utc>,
}

impl DatasetSnapshot {
    pub fn new(
        dataset_id: Uuid,
        session_id: Uuid,
        storage_ref: impl Into<String>,
        row_count: u64,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            dataset_id,
            session_id,
            storage_ref: storage_ref.into(),
            row_count,
            quality_score: None,
            lineage: Vec::new(),
            schema_fields: Vec::new(),
            schema: None,
            created_at: Utc::now(),
        }
    }

    pub fn with_schema(mut self, schema: Arc<Schema>) -> ConnectorResult<Self> {
        self.schema_fields = schema
            .fields()
            .iter()
            .map(|field| SchemaFieldSnapshot::from_field(field.as_ref()))
            .collect();
        self.schema = Some(schema);
        Ok(self)
    }

    pub fn resolved_schema(&self) -> Option<Arc<Schema>> {
        if let Some(schema) = &self.schema {
            return Some(schema.clone());
        }
        if self.schema_fields.is_empty() {
            return None;
        }
        let fields: Vec<Field> = self
            .schema_fields
            .iter()
            .map(SchemaFieldSnapshot::to_field)
            .collect();
        Some(Arc::new(Schema::new(fields)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_roundtrips_through_schema_fields() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let snapshot = DatasetSnapshot::new(Uuid::new_v4(), Uuid::new_v4(), "snap://1", 10)
            .with_schema(schema)
            .expect("schema");
        let json = serde_json::to_string(&snapshot).expect("serialize");
        let restored: DatasetSnapshot = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(restored.schema_fields.len(), 2);
        assert!(restored.resolved_schema().is_some());
    }
}
