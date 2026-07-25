use std::{
    collections::HashMap,
    io,
    path::{Path, PathBuf},
};

use tokio::{fs, sync::RwLock};
use uuid::Uuid;

use crate::models::StoredDataset;

pub struct Storage {
    root: PathBuf,
    index_path: PathBuf,
    datasets: RwLock<HashMap<Uuid, StoredDataset>>,
}

impl Storage {
    pub async fn open(root: impl AsRef<Path>) -> io::Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(root.join("uploads")).await?;
        fs::create_dir_all(root.join("exports")).await?;

        let index_path = root.join("datasets.json");
        let datasets = match fs::read(&index_path).await {
            Ok(bytes) => {
                let records: Vec<StoredDataset> = serde_json::from_slice(&bytes)
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
                records
                    .into_iter()
                    .map(|record| (record.id, record))
                    .collect()
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => HashMap::new(),
            Err(error) => return Err(error),
        };

        Ok(Self {
            root,
            index_path,
            datasets: RwLock::new(datasets),
        })
    }

    pub async fn list(&self) -> Vec<StoredDataset> {
        let mut records: Vec<_> = self.datasets.read().await.values().cloned().collect();
        records.sort_by(|left, right| right.created_at.cmp(&left.created_at));
        records
    }

    pub async fn get(&self, id: Uuid) -> Option<StoredDataset> {
        self.datasets.read().await.get(&id).cloned()
    }

    pub async fn insert(&self, dataset: StoredDataset) -> io::Result<()> {
        let mut datasets = self.datasets.write().await;
        datasets.insert(dataset.id, dataset);

        let mut records: Vec<_> = datasets.values().cloned().collect();
        records.sort_by(|left, right| right.created_at.cmp(&left.created_at));
        let bytes = serde_json::to_vec_pretty(&records)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;

        let temporary_path = self.index_path.with_extension("json.tmp");
        fs::write(&temporary_path, bytes).await?;
        fs::rename(temporary_path, &self.index_path).await
    }

    pub fn upload_path(&self, id: Uuid) -> PathBuf {
        self.root.join("uploads").join(format!("{id}.csv"))
    }

    pub fn export_path(&self, id: Uuid) -> PathBuf {
        self.root.join("exports").join(format!("{id}.csv"))
    }

    pub fn resolve(&self, dataset: &StoredDataset) -> PathBuf {
        self.root.join(&dataset.storage_path)
    }
}
