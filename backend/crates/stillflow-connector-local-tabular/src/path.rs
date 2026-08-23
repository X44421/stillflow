use std::collections::{BTreeMap, HashSet};
use std::fs::{File, Metadata};
use std::path::{Component, Path};

#[cfg(windows)]
use std::path::PathBuf;

use cap_fs_ext::{DirExt, FollowSymlinks, OpenOptionsFollowExt};
use cap_std::ambient_authority;
use cap_std::fs::{Dir, OpenOptions};
use chrono::{DateTime, Utc};
use stillflow_core::{
    AssetKind, AssetLocator, ConnectorError, ConnectorResult, RequestContext, SourceAsset,
};
use uuid::Uuid;

use crate::config::LocalTabularConfig;
use crate::format::TabularFormat;

const ASSET_NAMESPACE: Uuid = Uuid::from_u128(0x9c264b8a_3218_5dc7_a51e_f8d53620f75d);

pub(crate) struct RootSet {
    roots: Vec<AllowedRoot>,
}

struct AllowedRoot {
    index: usize,
    identity_key: String,
    specificity: usize,
    dir: Dir,
}

pub(crate) struct OpenedAsset {
    pub(crate) file: File,
    pub(crate) format: TabularFormat,
    pub(crate) size_bytes: u64,
    pub(crate) modified_at: Option<DateTime<Utc>>,
}

struct Candidate {
    root_index: usize,
    root_identity: String,
    relative: String,
    name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum FileIdentity {
    #[cfg(unix)]
    DeviceInode(u64, u64),
    NormalizedPath(String),
}

impl RootSet {
    pub(crate) fn open(config: &LocalTabularConfig) -> ConnectorResult<Self> {
        let mut roots = Vec::new();
        let mut seen = HashSet::new();
        for configured in &config.allowed_roots {
            let identity_key = normalized_root_key(configured)?;
            if !seen.insert(platform_comparison_key(&identity_key)) {
                continue;
            }
            let specificity = configured
                .components()
                .filter(|component| matches!(component, Component::Normal(_)))
                .count();
            let dir = open_absolute_dir_nofollow(configured)?;
            dir.entries().map_err(|_| {
                ConnectorError::with_category(
                    stillflow_core::ErrorCategory::Authorization,
                    false,
                    "an allowed root is not readable",
                    Vec::new(),
                    BTreeMap::new(),
                )
            })?;
            roots.push(AllowedRoot {
                index: roots.len(),
                identity_key,
                specificity,
                dir,
            });
        }
        if roots.is_empty() {
            return Err(ConnectorError::invalid_configuration(
                "allowedRoots did not contain a unique readable directory",
            ));
        }
        Ok(Self { roots })
    }

    pub(crate) fn discover(
        &self,
        connection_id: Uuid,
        parent: Option<&str>,
        context: &RequestContext,
        max_depth: usize,
        max_assets: usize,
    ) -> ConnectorResult<Vec<SourceAsset>> {
        context.ensure_active()?;
        let selected = self.discovery_roots(parent, max_depth)?;
        let mut order = selected;
        order.sort_by_key(|(root, _, _)| (std::cmp::Reverse(root.specificity), root.index));

        let mut unique = BTreeMap::new();
        for (root, relative_parent, initial_depth) in order {
            context.ensure_active()?;
            let directory = open_relative_dir(&root.dir, &relative_parent)?;
            walk_directory(
                root,
                &directory,
                &relative_parent,
                initial_depth,
                max_depth,
                max_assets,
                context,
                &mut unique,
            )?;
        }

        let discovered_at = Utc::now();
        let mut assets = unique
            .into_values()
            .map(|candidate| {
                let identity_name = format!("{}\0{}", candidate.root_identity, candidate.relative);
                (
                    candidate.root_index,
                    SourceAsset {
                        id: Uuid::new_v5(&ASSET_NAMESPACE, identity_name.as_bytes()),
                        connection_id,
                        kind: AssetKind::File,
                        name: candidate.name,
                        locator: AssetLocator {
                            path: candidate.relative,
                            container: Some(root_label(candidate.root_index)),
                            schema: None,
                            sheet: None,
                            workbook_region: None,
                        },
                        discovered_at,
                    },
                )
            })
            .collect::<Vec<_>>();
        assets.sort_by(|(left_root, left), (right_root, right)| {
            left_root
                .cmp(right_root)
                .then_with(|| left.locator.path.cmp(&right.locator.path))
        });
        Ok(assets.into_iter().map(|(_, asset)| asset).collect())
    }

    pub(crate) fn open_asset(&self, asset: &SourceAsset) -> ConnectorResult<OpenedAsset> {
        if asset.kind != AssetKind::File {
            return Err(ConnectorError::invalid_configuration(
                "local tabular assets must have file kind",
            ));
        }
        let root_index = parse_root_label(asset.locator.container.as_deref())?;
        let root = self.roots.get(root_index).ok_or_else(|| {
            ConnectorError::invalid_configuration("asset refers to an unknown allowed root")
        })?;
        let components = validate_relative_locator(&asset.locator.path)?;
        let (file, metadata) = open_relative_file(&root.dir, &components)?;
        let modified_at = metadata.modified().ok().map(DateTime::<Utc>::from);
        Ok(OpenedAsset {
            file,
            format: TabularFormat::from_locator(&asset.locator.path)?,
            size_bytes: metadata.len(),
            modified_at,
        })
    }

    fn discovery_roots(
        &self,
        parent: Option<&str>,
        max_depth: usize,
    ) -> ConnectorResult<Vec<(&AllowedRoot, Vec<String>, usize)>> {
        let Some(parent) = parent else {
            return Ok(self
                .roots
                .iter()
                .map(|root| (root, Vec::new(), 0))
                .collect());
        };
        if parent.is_empty() {
            return Err(ConnectorError::invalid_configuration(
                "discovery parent path must not be empty",
            ));
        }

        let mut parts = validate_relative_locator(parent)?;
        let explicit_root = parts
            .first()
            .and_then(|value| value.strip_prefix("root-"))
            .and_then(|value| value.parse::<usize>().ok());
        let root_index = match explicit_root {
            Some(index) => {
                parts.remove(0);
                index
            }
            None if self.roots.len() == 1 => 0,
            None => {
                return Err(ConnectorError::invalid_configuration(
                    "discovery parent must name an allowed root",
                ))
            }
        };
        let root = self.roots.get(root_index).ok_or_else(|| {
            ConnectorError::invalid_configuration("discovery parent names an unknown root")
        })?;
        if parts.len() > max_depth {
            return Err(ConnectorError::invalid_configuration(
                "discovery parent exceeds maxDiscoveryDepth",
            ));
        }
        let initial_depth = parts.len();
        Ok(vec![(root, parts, initial_depth)])
    }
}

#[allow(clippy::too_many_arguments)]
fn walk_directory(
    root: &AllowedRoot,
    directory: &Dir,
    relative_parent: &[String],
    depth: usize,
    max_depth: usize,
    max_assets: usize,
    context: &RequestContext,
    unique: &mut BTreeMap<FileIdentity, Candidate>,
) -> ConnectorResult<()> {
    context.ensure_active()?;
    let entries = directory.entries().map_err(|_| {
        source_error(
            stillflow_core::ErrorCategory::Authorization,
            false,
            "a directory could not be enumerated",
        )
    })?;
    let mut entries = entries
        .map(|entry| {
            let entry = entry.map_err(|_| {
                source_error(
                    stillflow_core::ErrorCategory::TransientSource,
                    true,
                    "a directory entry could not be read",
                )
            })?;
            let name = entry.file_name().into_string().map_err(|_| {
                source_error(
                    stillflow_core::ErrorCategory::InvalidData,
                    false,
                    "a local asset name is not valid UTF-8",
                )
            })?;
            Ok((name, entry))
        })
        .collect::<ConnectorResult<Vec<_>>>()?;
    entries.sort_by(|left, right| left.0.cmp(&right.0));

    for (name, entry) in entries {
        context.ensure_active()?;
        if matches!(name.as_str(), "." | "..") || name.contains(['/', '\\', '\0']) {
            return Err(source_error(
                stillflow_core::ErrorCategory::InvalidData,
                false,
                "a local asset name is not representable as a safe locator",
            ));
        }
        let file_type = entry.file_type().map_err(|_| {
            source_error(
                stillflow_core::ErrorCategory::TransientSource,
                true,
                "a local asset type could not be inspected",
            )
        })?;
        if file_type.is_symlink() {
            continue;
        }
        if file_type.is_dir() {
            if depth >= max_depth {
                let child = directory.open_dir_nofollow(&name).map_err(|_| {
                    source_error(
                        stillflow_core::ErrorCategory::InvalidConfiguration,
                        false,
                        "discovery refused a linked or unreadable directory",
                    )
                })?;
                if directory_may_hide_supported_asset(&child, context)? {
                    return Err(source_error(
                        stillflow_core::ErrorCategory::InvalidData,
                        false,
                        "discovery exceeded maxDiscoveryDepth",
                    ));
                }
                continue;
            }
            let child = directory.open_dir_nofollow(&name).map_err(|_| {
                source_error(
                    stillflow_core::ErrorCategory::InvalidConfiguration,
                    false,
                    "discovery refused a linked or unreadable directory",
                )
            })?;
            let mut child_parent = relative_parent.to_vec();
            child_parent.push(name);
            walk_directory(
                root,
                &child,
                &child_parent,
                depth + 1,
                max_depth,
                max_assets,
                context,
                unique,
            )?;
            continue;
        }
        if !file_type.is_file() || !TabularFormat::supports_file_name(&name) {
            continue;
        }

        let mut components = relative_parent.to_vec();
        components.push(name.clone());
        let relative = components.join("/");
        let (file, metadata) = open_relative_file(directory, std::slice::from_ref(&name))?;
        drop(file);
        let identity = file_identity(&metadata, &root.identity_key, &relative);
        unique.entry(identity.clone()).or_insert(Candidate {
            root_index: root.index,
            root_identity: root.identity_key.clone(),
            relative,
            name,
        });
        if unique.len() > max_assets {
            return Err(source_error(
                stillflow_core::ErrorCategory::InvalidData,
                false,
                "discovery exceeded maxDiscoveredAssets",
            ));
        }
    }
    Ok(())
}

fn directory_may_hide_supported_asset(
    directory: &Dir,
    context: &RequestContext,
) -> ConnectorResult<bool> {
    let entries = directory.entries().map_err(|_| {
        source_error(
            stillflow_core::ErrorCategory::Authorization,
            false,
            "a directory beyond the discovery depth could not be inspected",
        )
    })?;
    for entry in entries {
        context.ensure_active()?;
        let entry = entry.map_err(|_| {
            source_error(
                stillflow_core::ErrorCategory::TransientSource,
                true,
                "a directory entry beyond the discovery depth could not be read",
            )
        })?;
        let file_type = entry.file_type().map_err(|_| {
            source_error(
                stillflow_core::ErrorCategory::TransientSource,
                true,
                "a local asset type beyond the discovery depth could not be inspected",
            )
        })?;
        if file_type.is_symlink() {
            continue;
        }
        if file_type.is_dir() {
            return Ok(true);
        }
        if file_type.is_file() {
            let Some(name) = entry.file_name().to_str().map(ToOwned::to_owned) else {
                return Ok(true);
            };
            if TabularFormat::supports_file_name(&name) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn open_relative_dir(root: &Dir, components: &[String]) -> ConnectorResult<Dir> {
    let mut current = Dir::reopen_dir(root).map_err(|_| {
        source_error(
            stillflow_core::ErrorCategory::TransientSource,
            true,
            "an allowed root handle could not be reopened",
        )
    })?;
    for component in components {
        current = current.open_dir_nofollow(component).map_err(|_| {
            ConnectorError::invalid_configuration(
                "locator traverses a linked, missing, or non-directory component",
            )
        })?;
    }
    Ok(current)
}

fn open_relative_file(root: &Dir, components: &[String]) -> ConnectorResult<(File, Metadata)> {
    let Some((file_name, parents)) = components.split_last() else {
        return Err(ConnectorError::invalid_configuration(
            "asset locator must identify a file",
        ));
    };
    let directory = open_relative_dir(root, parents)?;
    let mut options = OpenOptions::new();
    options.read(true).follow(FollowSymlinks::No);
    let file = directory.open_with(file_name, &options).map_err(|error| {
        let category = match error.kind() {
            std::io::ErrorKind::NotFound => stillflow_core::ErrorCategory::NotFound,
            std::io::ErrorKind::PermissionDenied => stillflow_core::ErrorCategory::Authorization,
            _ => stillflow_core::ErrorCategory::InvalidConfiguration,
        };
        source_error(category, false, "asset could not be opened safely")
    })?;
    let file = file.into_std();
    let metadata = file.metadata().map_err(|_| {
        source_error(
            stillflow_core::ErrorCategory::TransientSource,
            true,
            "asset metadata could not be read",
        )
    })?;
    if !metadata.is_file() {
        return Err(ConnectorError::invalid_configuration(
            "asset locator does not identify a regular file",
        ));
    }
    Ok((file, metadata))
}

fn open_absolute_dir_nofollow(path: &Path) -> ConnectorResult<Dir> {
    if !path.is_absolute() {
        return Err(ConnectorError::invalid_configuration(
            "allowedRoots entries must be absolute paths",
        ));
    }
    #[cfg(unix)]
    {
        let mut components = path.components();
        if !matches!(components.next(), Some(Component::RootDir)) {
            return Err(ConnectorError::invalid_configuration(
                "allowed root has an invalid absolute prefix",
            ));
        }
        let mut directory = Dir::open_ambient_dir("/", ambient_authority()).map_err(|_| {
            ConnectorError::invalid_configuration("filesystem root could not be opened")
        })?;
        for component in components {
            let Component::Normal(name) = component else {
                return Err(ConnectorError::invalid_configuration(
                    "allowed root contains a forbidden path component",
                ));
            };
            directory = directory.open_dir_nofollow(name).map_err(|_| {
                ConnectorError::invalid_configuration(
                    "allowed root contains a linked, missing, or non-directory component",
                )
            })?;
        }
        Ok(directory)
    }
    #[cfg(windows)]
    {
        let mut components = path.components();
        let Some(Component::Prefix(prefix)) = components.next() else {
            return Err(ConnectorError::invalid_configuration(
                "allowed root must contain a drive or UNC prefix",
            ));
        };
        if !matches!(components.next(), Some(Component::RootDir)) {
            return Err(ConnectorError::invalid_configuration(
                "allowed root prefix must be absolute",
            ));
        }
        let mut anchor = PathBuf::from(prefix.as_os_str());
        anchor.push("\\");
        let mut directory = Dir::open_ambient_dir(anchor, ambient_authority()).map_err(|_| {
            ConnectorError::invalid_configuration("filesystem root could not be opened")
        })?;
        for component in components {
            let Component::Normal(name) = component else {
                return Err(ConnectorError::invalid_configuration(
                    "allowed root contains a forbidden path component",
                ));
            };
            directory = directory.open_dir_nofollow(name).map_err(|_| {
                ConnectorError::invalid_configuration(
                    "allowed root contains a linked, missing, or non-directory component",
                )
            })?;
        }
        Ok(directory)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = path;
        Err(ConnectorError::invalid_configuration(
            "this platform does not provide no-follow directory traversal",
        ))
    }
}

fn normalized_root_key(path: &Path) -> ConnectorResult<String> {
    if !path.is_absolute() {
        return Err(ConnectorError::invalid_configuration(
            "allowedRoots entries must be absolute paths",
        ));
    }
    for component in path.components() {
        if matches!(component, Component::CurDir | Component::ParentDir) {
            return Err(ConnectorError::invalid_configuration(
                "allowed root contains a forbidden path component",
            ));
        }
    }
    let normalized = path.components().collect::<std::path::PathBuf>();
    normalized
        .to_str()
        .filter(|value| !value.contains('\0'))
        .map(ToOwned::to_owned)
        .ok_or_else(|| ConnectorError::invalid_configuration("allowed root must be valid UTF-8"))
}

fn platform_comparison_key(value: &str) -> String {
    #[cfg(windows)]
    {
        value.replace('\\', "/").to_lowercase()
    }
    #[cfg(not(windows))]
    {
        value.to_owned()
    }
}

fn validate_relative_locator(locator: &str) -> ConnectorResult<Vec<String>> {
    if locator.is_empty()
        || locator.contains(['\\', '\0'])
        || locator.starts_with('/')
        || locator.starts_with("//")
    {
        return Err(ConnectorError::invalid_configuration(
            "asset locator is not a safe root-relative path",
        ));
    }
    let components = locator
        .split('/')
        .map(|component| {
            if component.is_empty()
                || matches!(component, "." | "..")
                || component.as_bytes().get(1) == Some(&b':')
            {
                return Err(ConnectorError::invalid_configuration(
                    "asset locator contains a forbidden path component",
                ));
            }
            Ok(component.to_owned())
        })
        .collect::<ConnectorResult<Vec<_>>>()?;
    Ok(components)
}

fn parse_root_label(label: Option<&str>) -> ConnectorResult<usize> {
    let label = label.ok_or_else(|| {
        ConnectorError::invalid_configuration("asset locator is missing its allowed-root label")
    })?;
    let value = label.strip_prefix("root-").ok_or_else(|| {
        ConnectorError::invalid_configuration("asset locator has an invalid allowed-root label")
    })?;
    let index = value.parse::<usize>().map_err(|_| {
        ConnectorError::invalid_configuration("asset locator has an invalid allowed-root label")
    })?;
    if root_label(index) != label {
        return Err(ConnectorError::invalid_configuration(
            "asset locator has a non-canonical allowed-root label",
        ));
    }
    Ok(index)
}

fn root_label(index: usize) -> String {
    format!("root-{index}")
}

fn file_identity(metadata: &Metadata, root: &str, relative: &str) -> FileIdentity {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let device = metadata.dev();
        let inode = metadata.ino();
        if device != 0 || inode != 0 {
            return FileIdentity::DeviceInode(device, inode);
        }
    }
    FileIdentity::NormalizedPath(platform_comparison_key(&format!("{root}/{relative}")))
}

fn source_error(
    category: stillflow_core::ErrorCategory,
    retryable: bool,
    message: &'static str,
) -> ConnectorError {
    ConnectorError::with_category(category, retryable, message, Vec::new(), BTreeMap::new())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::TempDir;

    use super::*;

    fn config(root: &Path) -> LocalTabularConfig {
        LocalTabularConfig {
            allowed_roots: vec![root.to_path_buf()],
            max_discovery_depth: 16,
            max_discovered_assets: 100,
            inference_rows: 100,
            inference_bytes: 1024,
            csv_delimiter: b',',
            csv_quote: b'"',
            csv_has_header: true,
            tsv_has_header: true,
        }
    }

    #[test]
    fn discovers_in_stable_order_and_rejects_malicious_locators() {
        let temp = TempDir::new().expect("temp directory");
        fs::create_dir(temp.path().join("nested")).expect("nested directory");
        fs::write(temp.path().join("z.csv"), b"id\n1\n").expect("CSV");
        fs::write(temp.path().join("nested/a.JSONL"), b"{\"id\":1}\n").expect("JSONL");
        fs::write(temp.path().join("ignored.txt"), b"ignored").expect("text");
        let roots = RootSet::open(&config(temp.path())).expect("roots");
        let assets = roots
            .discover(
                Uuid::from_u128(1),
                None,
                &RequestContext::default(),
                16,
                100,
            )
            .expect("discover");
        assert_eq!(
            assets
                .iter()
                .map(|asset| asset.locator.path.as_str())
                .collect::<Vec<_>>(),
            ["nested/a.JSONL", "z.csv"]
        );
        let repeat = roots
            .discover(
                Uuid::from_u128(1),
                None,
                &RequestContext::default(),
                16,
                100,
            )
            .expect("repeat");
        assert_eq!(assets[0].id, repeat[0].id);

        for locator in [
            "../outside.csv",
            "/absolute.csv",
            "//server/share.csv",
            "C:/drive.csv",
            "nested\\ambiguous.csv",
            "nested//empty.csv",
            "./current.csv",
            "nested/../outside.csv",
            "nul\0byte.csv",
        ] {
            let mut malicious = assets[0].clone();
            malicious.locator.path = locator.to_owned();
            assert!(roots.open_asset(&malicious).is_err(), "{locator}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn refuses_symlinked_roots_directories_and_files() {
        use std::os::unix::fs::symlink;

        let temp = TempDir::new().expect("temp directory");
        let real = temp.path().join("real");
        fs::create_dir(&real).expect("real directory");
        fs::write(real.join("data.csv"), b"id\n1\n").expect("CSV");
        let linked_root = temp.path().join("linked-root");
        symlink(&real, &linked_root).expect("root symlink");
        assert!(RootSet::open(&config(&linked_root)).is_err());

        let roots = RootSet::open(&config(&real)).expect("roots");
        symlink(real.join("data.csv"), real.join("linked.csv")).expect("file symlink");
        let assets = roots
            .discover(
                Uuid::from_u128(1),
                None,
                &RequestContext::default(),
                16,
                100,
            )
            .expect("discover");
        assert_eq!(assets.len(), 1);
    }

    #[test]
    fn fails_instead_of_returning_partial_depth_or_asset_results() {
        let temp = TempDir::new().expect("temp directory");
        fs::create_dir(temp.path().join("nested")).expect("nested directory");
        fs::write(temp.path().join("nested/data.csv"), b"id\n1\n").expect("nested CSV");
        fs::write(temp.path().join("first.csv"), b"id\n1\n").expect("first CSV");
        fs::write(temp.path().join("second.csv"), b"id\n2\n").expect("second CSV");
        let roots = RootSet::open(&config(temp.path())).expect("roots");
        assert!(roots
            .discover(Uuid::from_u128(1), None, &RequestContext::default(), 0, 100,)
            .is_err());
        assert!(roots
            .discover(Uuid::from_u128(1), None, &RequestContext::default(), 16, 1,)
            .is_err());

        let ignored = TempDir::new().expect("ignored fixture root");
        fs::create_dir(ignored.path().join("nested")).expect("ignored directory");
        fs::write(ignored.path().join("nested/readme.txt"), b"ignored").expect("ignored file");
        fs::write(ignored.path().join("data.csv"), b"id\n1\n").expect("root CSV");
        let roots = RootSet::open(&config(ignored.path())).expect("ignored roots");
        let assets = roots
            .discover(Uuid::from_u128(1), None, &RequestContext::default(), 0, 100)
            .expect("irrelevant deeper entries do not truncate results");
        assert_eq!(assets.len(), 1);
    }

    #[test]
    fn overlapping_roots_choose_the_most_specific_identity() {
        let temp = TempDir::new().expect("temp directory");
        let nested = temp.path().join("nested");
        fs::create_dir(&nested).expect("nested directory");
        fs::write(nested.join("data.csv"), b"id\n1\n").expect("CSV");
        let mut overlapping = config(temp.path());
        overlapping.allowed_roots.push(nested.clone());
        let roots = RootSet::open(&overlapping).expect("overlapping roots");
        let assets = roots
            .discover(
                Uuid::from_u128(1),
                None,
                &RequestContext::default(),
                16,
                100,
            )
            .expect("discover");
        assert_eq!(assets.len(), 1);
        assert_eq!(assets[0].locator.container.as_deref(), Some("root-1"));
        assert_eq!(assets[0].locator.path, "data.csv");

        let duplicate_with_separator = format!("{}/", nested.display());
        let mut duplicates = config(&nested);
        duplicates
            .allowed_roots
            .push(std::path::PathBuf::from(duplicate_with_separator));
        let roots = RootSet::open(&duplicates).expect("deduplicated roots");
        assert_eq!(roots.roots.len(), 1);
    }

    #[test]
    fn orders_double_digit_root_labels_by_numeric_precedence() {
        let temp = TempDir::new().expect("temp directory");
        let mut roots = Vec::new();
        for index in 0..11 {
            let root = temp.path().join(format!("root-{index:02}"));
            fs::create_dir(&root).expect("allowed root");
            fs::write(root.join("data.csv"), b"id\n1\n").expect("CSV");
            roots.push(root);
        }
        let mut configuration = config(&roots[0]);
        configuration.allowed_roots = roots;
        let roots = RootSet::open(&configuration).expect("roots");
        let assets = roots
            .discover(
                Uuid::from_u128(1),
                None,
                &RequestContext::default(),
                16,
                100,
            )
            .expect("discover");
        assert_eq!(
            assets
                .iter()
                .filter_map(|asset| asset.locator.container.clone())
                .collect::<Vec<_>>(),
            (0..11).map(root_label).collect::<Vec<_>>()
        );
    }

    #[test]
    fn validates_roots_and_applies_depth_from_the_allowed_root() {
        let temp = TempDir::new().expect("temp directory");
        let relative = config(Path::new("relative"));
        assert!(RootSet::open(&relative).is_err());

        let missing_path = temp.path().join("missing");
        let missing = config(&missing_path);
        assert!(RootSet::open(&missing).is_err());

        let file_path = temp.path().join("not-a-directory");
        fs::write(&file_path, b"file").expect("file root");
        let file = config(&file_path);
        assert!(RootSet::open(&file).is_err());

        let nested = temp.path().join("nested");
        fs::create_dir(&nested).expect("nested directory");
        fs::write(nested.join("data.csv"), b"id\n1\n").expect("CSV");
        let roots = RootSet::open(&config(temp.path())).expect("roots");
        assert!(roots
            .discover(
                Uuid::from_u128(1),
                Some("nested"),
                &RequestContext::default(),
                0,
                100,
            )
            .is_err());
    }
}
