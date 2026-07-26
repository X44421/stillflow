use std::{
    collections::HashMap,
    io,
    path::{Path, PathBuf},
};

use chrono::Utc;
use serde::{de::DeserializeOwned, Serialize};
use tokio::{fs, sync::RwLock};
use uuid::Uuid;

use crate::models::{ProjectNodeSnapshot, StoredDataset, StoredProject};

pub struct Storage {
    root: PathBuf,
    datasets_index_path: PathBuf,
    projects_index_path: PathBuf,
    datasets: RwLock<HashMap<Uuid, StoredDataset>>,
    projects: RwLock<HashMap<Uuid, StoredProject>>,
}

impl Storage {
    pub async fn open(root: impl AsRef<Path>) -> io::Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(root.join("uploads")).await?;
        fs::create_dir_all(root.join("exports")).await?;

        let datasets_index_path = root.join("datasets.json");
        let projects_index_path = root.join("projects.json");
        let mut datasets = load_records::<StoredDataset>(&datasets_index_path)
            .await?
            .into_iter()
            .map(|record| (record.id, record))
            .collect::<HashMap<_, _>>();
        let mut projects = load_records::<StoredProject>(&projects_index_path)
            .await?
            .into_iter()
            .map(|record| (record.id, record))
            .collect::<HashMap<_, _>>();

        let mut projects_changed = false;
        if projects.is_empty() {
            let now = Utc::now();
            let project = StoredProject {
                id: Uuid::new_v4(),
                name: "Customer Data Cleaning".to_owned(),
                description: String::new(),
                selected_dataset_id: None,
                latest_output_id: None,
                nodes: Vec::new(),
                created_at: now,
                updated_at: now,
            };
            projects.insert(project.id, project);
            projects_changed = true;
        }

        let default_project_id = projects
            .values()
            .min_by_key(|project| project.created_at)
            .expect("a default project is always available")
            .id;
        let mut datasets_changed = false;
        for dataset in datasets.values_mut() {
            let project_is_missing = dataset
                .project_id
                .is_some_and(|project_id| !projects.contains_key(&project_id));
            if dataset.project_id.is_none() || project_is_missing {
                dataset.project_id = Some(default_project_id);
                datasets_changed = true;
            }
        }

        if projects_changed {
            persist_projects(&projects_index_path, &projects).await?;
        }
        if datasets_changed {
            persist_datasets(&datasets_index_path, &datasets).await?;
        }

        Ok(Self {
            root,
            datasets_index_path,
            projects_index_path,
            datasets: RwLock::new(datasets),
            projects: RwLock::new(projects),
        })
    }

    pub async fn list_datasets(&self, project_id: Option<Uuid>) -> Vec<StoredDataset> {
        let mut records: Vec<_> = self
            .datasets
            .read()
            .await
            .values()
            .filter(|dataset| {
                project_id.is_none() || dataset.project_id == project_id
            })
            .cloned()
            .collect();
        records.sort_by(|left, right| right.created_at.cmp(&left.created_at));
        records
    }

    pub async fn get_dataset(&self, id: Uuid) -> Option<StoredDataset> {
        self.datasets.read().await.get(&id).cloned()
    }

    pub async fn insert_dataset(&self, dataset: StoredDataset) -> io::Result<()> {
        let projects = self.projects.read().await;
        if dataset
            .project_id
            .is_some_and(|project_id| !projects.contains_key(&project_id))
        {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "dataset project does not exist",
            ));
        }
        let mut datasets = self.datasets.write().await;
        let mut next = datasets.clone();
        next.insert(dataset.id, dataset);
        persist_datasets(&self.datasets_index_path, &next).await?;
        *datasets = next;
        drop(projects);
        Ok(())
    }

    pub async fn rename_dataset(
        &self,
        id: Uuid,
        name: String,
    ) -> io::Result<Option<StoredDataset>> {
        let mut datasets = self.datasets.write().await;
        let mut next = datasets.clone();
        let Some(dataset) = next.get_mut(&id) else {
            return Ok(None);
        };
        dataset.name = name;
        let updated = dataset.clone();
        persist_datasets(&self.datasets_index_path, &next).await?;
        *datasets = next;
        Ok(Some(updated))
    }

    pub async fn remove_dataset(&self, id: Uuid) -> io::Result<Option<StoredDataset>> {
        let mut projects = self.projects.write().await;
        let mut datasets = self.datasets.write().await;
        let mut next_datasets = datasets.clone();
        let Some(removed) = next_datasets.remove(&id) else {
            return Ok(None);
        };

        let mut next_projects = projects.clone();
        let mut projects_changed = false;
        for project in next_projects.values_mut() {
            let referenced =
                project.selected_dataset_id == Some(id) || project.latest_output_id == Some(id);
            if !referenced {
                continue;
            }
            if project.selected_dataset_id == Some(id) {
                project.selected_dataset_id = None;
            }
            if project.latest_output_id == Some(id) {
                project.latest_output_id = None;
            }
            project.updated_at = Utc::now();
            projects_changed = true;
        }

        persist_datasets(&self.datasets_index_path, &next_datasets).await?;
        if projects_changed {
            persist_projects(&self.projects_index_path, &next_projects).await?;
        }
        *datasets = next_datasets;
        if projects_changed {
            *projects = next_projects;
        }
        Ok(Some(removed))
    }

    pub async fn list_projects(&self) -> Vec<StoredProject> {
        let mut records: Vec<_> = self.projects.read().await.values().cloned().collect();
        records.sort_by(|left, right| right.updated_at.cmp(&left.updated_at));
        records
    }

    pub async fn get_project(&self, id: Uuid) -> Option<StoredProject> {
        self.projects.read().await.get(&id).cloned()
    }

    pub async fn project_count(&self) -> usize {
        self.projects.read().await.len()
    }

    pub async fn insert_project(&self, project: StoredProject) -> io::Result<()> {
        let mut projects = self.projects.write().await;
        let mut next = projects.clone();
        next.insert(project.id, project);
        persist_projects(&self.projects_index_path, &next).await?;
        *projects = next;
        Ok(())
    }

    pub async fn update_project(
        &self,
        id: Uuid,
        name: Option<String>,
        description: Option<String>,
    ) -> io::Result<Option<StoredProject>> {
        let mut projects = self.projects.write().await;
        let mut next = projects.clone();
        let Some(project) = next.get_mut(&id) else {
            return Ok(None);
        };
        if let Some(name) = name {
            project.name = name;
        }
        if let Some(description) = description {
            project.description = description;
        }
        project.updated_at = Utc::now();
        let updated = project.clone();
        persist_projects(&self.projects_index_path, &next).await?;
        *projects = next;
        Ok(Some(updated))
    }

    pub async fn save_project_workspace(
        &self,
        id: Uuid,
        selected_dataset_id: Option<Uuid>,
        latest_output_id: Option<Uuid>,
        nodes: Vec<ProjectNodeSnapshot>,
    ) -> io::Result<Option<StoredProject>> {
        let mut projects = self.projects.write().await;
        let mut next = projects.clone();
        let Some(project) = next.get_mut(&id) else {
            return Ok(None);
        };
        project.selected_dataset_id = selected_dataset_id;
        project.latest_output_id = latest_output_id;
        project.nodes = nodes;
        project.updated_at = Utc::now();
        let updated = project.clone();
        persist_projects(&self.projects_index_path, &next).await?;
        *projects = next;
        Ok(Some(updated))
    }

    pub async fn update_project_dataset_state(
        &self,
        id: Uuid,
        selected_dataset_id: Option<Uuid>,
        latest_output_id: Option<Uuid>,
    ) -> io::Result<Option<StoredProject>> {
        let mut projects = self.projects.write().await;
        let mut next = projects.clone();
        let Some(project) = next.get_mut(&id) else {
            return Ok(None);
        };
        project.selected_dataset_id = selected_dataset_id;
        project.latest_output_id = latest_output_id;
        project.updated_at = Utc::now();
        let updated = project.clone();
        persist_projects(&self.projects_index_path, &next).await?;
        *projects = next;
        Ok(Some(updated))
    }

    pub async fn remove_project(
        &self,
        id: Uuid,
    ) -> io::Result<Option<(StoredProject, Vec<StoredDataset>)>> {
        let mut projects = self.projects.write().await;
        if projects.len() <= 1 {
            return Ok(None);
        }
        let mut datasets = self.datasets.write().await;
        let mut next_projects = projects.clone();
        let Some(removed_project) = next_projects.remove(&id) else {
            return Ok(None);
        };

        let mut next_datasets = datasets.clone();
        let removed_datasets = next_datasets
            .values()
            .filter(|dataset| dataset.project_id == Some(id))
            .cloned()
            .collect::<Vec<_>>();
        next_datasets.retain(|_, dataset| dataset.project_id != Some(id));

        persist_datasets(&self.datasets_index_path, &next_datasets).await?;
        persist_projects(&self.projects_index_path, &next_projects).await?;
        *datasets = next_datasets;
        *projects = next_projects;
        Ok(Some((removed_project, removed_datasets)))
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

async fn load_records<T: DeserializeOwned>(path: &Path) -> io::Result<Vec<T>> {
    match fs::read(path).await {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(error) => Err(error),
    }
}

async fn persist_datasets(
    path: &Path,
    datasets: &HashMap<Uuid, StoredDataset>,
) -> io::Result<()> {
    let mut records = datasets.values().cloned().collect::<Vec<_>>();
    records.sort_by(|left, right| right.created_at.cmp(&left.created_at));
    write_records(path, &records).await
}

async fn persist_projects(
    path: &Path,
    projects: &HashMap<Uuid, StoredProject>,
) -> io::Result<()> {
    let mut records = projects.values().cloned().collect::<Vec<_>>();
    records.sort_by(|left, right| right.updated_at.cmp(&left.updated_at));
    write_records(path, &records).await
}

async fn write_records<T: Serialize>(path: &Path, records: &[T]) -> io::Result<()> {
    let bytes = serde_json::to_vec_pretty(records)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let temporary_path = path.with_extension("json.tmp");
    fs::write(&temporary_path, bytes).await?;
    fs::rename(temporary_path, path).await
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::Storage;

    #[tokio::test]
    async fn creates_and_reloads_default_project() {
        let directory = tempdir().expect("temporary data directory");
        let project_id = {
            let storage = Storage::open(directory.path())
                .await
                .expect("open storage");
            let projects = storage.list_projects().await;
            assert_eq!(projects.len(), 1);
            assert_eq!(projects[0].name, "Customer Data Cleaning");
            projects[0].id
        };

        let reopened = Storage::open(directory.path())
            .await
            .expect("reopen storage");
        assert_eq!(reopened.list_projects().await[0].id, project_id);
    }
}
