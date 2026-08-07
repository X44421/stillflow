#[cfg(test)]
mod serde_roundtrip_tests {
    use std::collections::BTreeSet;

    use chrono::DateTime;
    use uuid::Uuid;

    use crate::{
        AssetKind, AssetLocator, Checkpoint, ColumnId, ConnectorKind, CredentialRef, Dataset,
        DatasetSnapshot, Expr, LogicalField, LogicalSchema, LogicalType, SamplingStrategy, Session,
        SnapshotStats, SourceAsset, SourceConnection, SourceFilter,
    };

    fn roundtrip<T>(value: &T) -> T
    where
        T: serde::Serialize + serde::de::DeserializeOwned,
    {
        let json = serde_json::to_string(value).expect("serialize");
        serde_json::from_str(&json).expect("deserialize")
    }

    #[test]
    fn source_connection_roundtrips_without_secrets() {
        let connection = SourceConnection::try_new(
            ConnectorKind::SqlDatabase,
            "warehouse",
            serde_json::json!({ "host": "db.example.com", "database": "analytics" }),
            CredentialRef::new("cred://vault/warehouse").expect("credential ref"),
        )
        .expect("connection");
        let restored = roundtrip(&connection);
        assert_eq!(restored.name(), connection.name());
        assert_eq!(restored.credential_ref(), connection.credential_ref());
    }

    #[test]
    fn source_asset_and_checkpoint_roundtrip() {
        let asset = SourceAsset::new(
            Uuid::new_v4(),
            AssetKind::Table,
            "orders",
            AssetLocator {
                path: "public.orders".to_owned(),
                container: None,
                schema: Some("public".to_owned()),
                sheet: None,
            },
        );
        let checkpoint = Checkpoint::new(1, b"resume".to_vec());
        assert_eq!(roundtrip(&asset).name, asset.name);
        assert_eq!(roundtrip(&checkpoint).token, checkpoint.token);
    }

    #[test]
    fn session_dataset_and_snapshot_roundtrip() {
        let connection_id = Uuid::new_v4();
        let session = Session::with_connection(connection_id);
        let source_asset_id = Uuid::new_v4();
        let dataset = Dataset::new(session.id, source_asset_id, "orders");
        let schema = LogicalSchema::new(vec![LogicalField::new(
            ColumnId::from_uuid(Uuid::from_u128(10)),
            "value",
            LogicalType::Int64,
            false,
        )
        .expect("field")])
        .expect("schema");
        let snapshot = DatasetSnapshot::try_new(
            Uuid::new_v4(),
            dataset.id,
            session.id,
            source_asset_id,
            schema,
            SnapshotStats::try_new(42, 1_024, 1).expect("stats"),
            BTreeSet::new(),
            None,
            DateTime::from_timestamp(1_700_000_000, 0).expect("timestamp"),
        )
        .expect("snapshot");
        assert_eq!(roundtrip(&session).connection_ids, session.connection_ids);
        assert_eq!(roundtrip(&dataset).name, dataset.name);
        assert_eq!(roundtrip(&snapshot), snapshot);
    }

    #[test]
    fn filter_and_sampling_strategy_roundtrip() {
        let filter =
            SourceFilter::new(Expr::Column(crate::ColumnId::from_uuid(Uuid::from_u128(1))))
                .expect("filter");
        assert_eq!(roundtrip(&filter).expression, filter.expression);
        assert_eq!(
            roundtrip(&SamplingStrategy::Reservoir),
            SamplingStrategy::Reservoir
        );
    }
}
