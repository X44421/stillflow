#[cfg(test)]
mod serde_roundtrip_tests {
    use chrono::Utc;
    use uuid::Uuid;

    use crate::{
        AssetKind, AssetLocator, Checkpoint, ConnectorKind, CredentialRef, Dataset,
        DatasetSnapshot, SamplingStrategy, Session, SourceAsset, SourceConnection, SourceFilter,
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
        let connection = SourceConnection {
            id: Uuid::new_v4(),
            kind: ConnectorKind::SqlDatabase,
            name: "warehouse".to_owned(),
            config: serde_json::json!({ "host": "db.example.com", "database": "analytics" }),
            credential_ref: CredentialRef::new("cred://vault/warehouse"),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let restored = roundtrip(&connection);
        assert_eq!(restored.name, connection.name);
        assert_eq!(restored.credential_ref, connection.credential_ref);
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
        let session = Session::new(Uuid::new_v4());
        let dataset = Dataset::new(session.id, Uuid::new_v4(), "orders");
        let snapshot = DatasetSnapshot::new(dataset.id, session.id, "snap://local/1", 42);
        assert_eq!(roundtrip(&session).connection_id, session.connection_id);
        assert_eq!(roundtrip(&dataset).name, dataset.name);
        assert_eq!(roundtrip(&snapshot).row_count, snapshot.row_count);
    }

    #[test]
    fn filter_and_sampling_strategy_roundtrip() {
        let filter = SourceFilter {
            expression: "status = 'open'".to_owned(),
        };
        assert_eq!(roundtrip(&filter).expression, filter.expression);
        assert_eq!(
            roundtrip(&SamplingStrategy::Reservoir),
            SamplingStrategy::Reservoir
        );
    }
}
