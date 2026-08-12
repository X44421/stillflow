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
    AssetKind, ConnectorError, ConnectorResult, ErrorCategory, RequestContext, SourceAsset,
};

use crate::config::WorkbookConfig;
use crate::format::WorkbookFormat;

pub(crate) struct RootSet {
    roots: Vec<AllowedRoot>,
}

struct AllowedRoot {
    index: usize,
    identity_key: String,
    specificity: usize,
    dir: Dir,
}

pub(crate) struct DiscoveredWorkbook {
    pub(crate) root_index: usize,
    pub(crate) root_identity: String,
    pub(crate) relative: String,
    pub(crate) name: String,
}

pub(crate) struct OpenedWorkbook {
    pub(crate) file: File,
    pub(crate) format: WorkbookFormat,
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
    pub(crate) fn open(config: &WorkbookConfig) -> ConnectorResult<Self> {
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
                source_error(
                    ErrorCategory::Authorization,
                    false,
                    "an allowed workbook root is not readable",
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

    pub(crate) fn discover_files(
        &self,
        parent: Option<&str>,
        context: &RequestContext,
        max_depth: usize,
        max_assets: usize,
    ) -> ConnectorResult<Vec<DiscoveredWorkbook>> {
        context.ensure_active()?;
        let mut order = self.discovery_roots(parent, max_depth)?;
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

        let mut files = unique
            .into_values()
            .map(|candidate| DiscoveredWorkbook {
                root_index: candidate.root_index,
                root_identity: candidate.root_identity,
                relative: candidate.relative,
                name: candidate.name,
            })
            .collect::<Vec<_>>();
        files.sort_by(|left, right| {
            left.root_index
                .cmp(&right.root_index)
                .then_with(|| left.relative.cmp(&right.relative))
        });
        Ok(files)
    }

    pub(crate) fn open_discovered(
        &self,
        discovered: &DiscoveredWorkbook,
    ) -> ConnectorResult<OpenedWorkbook> {
        self.open_locator(discovered.root_index, &discovered.relative)
    }

    pub(crate) fn open_asset(&self, asset: &SourceAsset) -> ConnectorResult<OpenedWorkbook> {
        if asset.kind != AssetKind::Sheet || asset.locator.sheet.is_none() {
            return Err(ConnectorError::invalid_configuration(
                "workbook assets must identify a sheet",
            ));
        }
        let root_index = parse_root_label(asset.locator.container.as_deref())?;
        self.open_locator(root_index, &asset.locator.path)
    }

    fn open_locator(&self, root_index: usize, locator: &str) -> ConnectorResult<OpenedWorkbook> {
        let root = self.roots.get(root_index).ok_or_else(|| {
            ConnectorError::invalid_configuration("asset refers to an unknown allowed root")
        })?;
        let components = validate_relative_locator(locator)?;
        let (file, metadata) = open_relative_file(&root.dir, &components)?;
        let modified_at = metadata.modified().ok().map(DateTime::<Utc>::from);
        Ok(OpenedWorkbook {
            file,
            format: WorkbookFormat::from_locator(locator)?,
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
            return Ok(self.roots.iter().map(|root| (root, Vec::new(), 0)).collect());
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
                ));
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
            ErrorCategory::Authorization,
            false,
            "a workbook directory could not be enumerated",
        )
    })?;
    let mut entries = entries
        .map(|entry| {
            let entry = entry.map_err(|_| {
                source_error(
                    ErrorCategory::TransientSource,
                    true,
                    "a workbook directory entry could not be read",
                )
            })?;
            let name = entry.file_name().into_string().map_err(|_| {
                source_error(
                    ErrorCategory::InvalidData,
                    false,
                    "a workbook asset name is not valid UTF-8",
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
                ErrorCategory::InvalidData,
                false,
                "a workbook asset name is not representable as a safe locator",
            ));
        }
        let file_type = entry.file_type().map_err(|_| {
            source_error(
                ErrorCategory::TransientSource,
                true,
                "a workbook asset type could not be inspected",
            )
        })?;
        if file_type.is_symlink() {
            continue;
        }
        if file_type.is_dir() {
            if depth >= max_depth {
                let child = directory.open_dir_nofollow(&name).map_err(|error| {
                    path_open_error(
                        error,
                        "discovery refused a linked or unreadable directory",
                    )
                })?;
                if directory_may_hide_supported_asset(&child, context)? {
                    return Err(source_error(
                        ErrorCategory::InvalidData,
                        false,
                        "discovery exceeded maxDiscoveryDepth",
                    ));
                }
                continue;
            }
            let child = directory.open_dir_nofollow(&name).map_err(|error| {
                path_open_error(
                    error,
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
        if !file_type.is_file() || !WorkbookFormat::supports_file_name(&name) {
            continue;
        }
        let mut components = relative_parent.to_vec();
        components.push(name.clone());
        let relative = components.join("/");
        let (file, metadata) = open_relative_file(directory, std::slice::from_ref(&name))?;
        drop(file);
        let identity = file_identity(&metadata, &root.identity_key, &relative);
        unique.entry(identity).or_insert(Candidate {
            root_index: root.index,
            root_identity: root.identity_key.clone(),
            relative,
            name,
        });
        if unique.len() > max_assets {
            return Err(source_error(
                ErrorCategory::InvalidData,
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
            ErrorCategory::Authorization,
            false,
            "a directory beyond the discovery depth could not be inspected",
        )
    })?;
    for entry in entries {
        context.ensure_active()?;
        let entry = entry.map_err(|_| {
            source_error(
                ErrorCategory::TransientSource,
                true,
                "a directory entry beyond the discovery depth could not be read",
            )
        })?;
        let file_type = entry.file_type().map_err(|_| {
            source_error(
                ErrorCategory::TransientSource,
                true,
                "a workbook asset type beyond the discovery depth could not be inspected",
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
            if WorkbookFormat::supports_file_name(&name) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn open_relative_dir(root: &Dir, components: &[String]) -> ConnectorResult<Dir> {
    let mut current = Dir::reopen_dir(root).map_err(|_| {
        source_error(
            ErrorCategory::TransientSource,
            true,
            "an allowed root handle could not be reopened",
        )
    })?;
    for component in components {
        current = current.open_dir_nofollow(component).map_err(|error| {
            path_open_error(
                error,
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
    let file = directory
        .open_with(file_name, &options)
        .map_err(|error| path_open_error(error, "workbook asset could not be opened safely"))?;
    let file = file.into_std();
    let metadata = file.metadata().map_err(|_| {
        source_error(
            ErrorCategory::TransientSource,
            true,
            "workbook asset metadata could not be read",
        )
    })?;
    if !metadata.is_file() {
        return Err(ConnectorError::invalid_configuration(
            "workbook locator does not identify a regular file",
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
        let mut directory = Dir::open_ambient_dir("/", ambient_authority()).map_err(|error| {
            path_open_error(error, "filesystem root could not be opened")
        })?;
        for component in components {
            let Component::Normal(name) = component else {
                return Err(ConnectorError::invalid_configuration(
                    "allowed root contains a forbidden path component",
                ));
            };
            directory = directory.open_dir_nofollow(name).map_err(|error| {
                path_open_error(
                    error,
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
        let mut directory = Dir::open_ambient_dir(anchor, ambient_authority()).map_err(|error| {
            path_open_error(error, "filesystem root could not be opened")
        })?;
        for component in components {
            let Component::Normal(name) = component else {
                return Err(ConnectorError::invalid_configuration(
                    "allowed root contains a forbidden path component",
                ));
            };
            directory = directory.open_dir_nofollow(name).map_err(|error| {
                path_open_error(
                    error,
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
            "workbook locator is not a safe root-relative path",
        ));
    }
    locator
        .split('/')
        .map(|component| {
            if component.is_empty()
                || matches!(component, "." | "..")
                || component.as_bytes().get(1) == Some(&b':')
            {
                return Err(ConnectorError::invalid_configuration(
                    "workbook locator contains a forbidden path component",
                ));
            }
            Ok(component.to_owned())
        })
        .collect()
}

fn parse_root_label(label: Option<&str>) -> ConnectorResult<usize> {
    let label = label.ok_or_else(|| {
        ConnectorError::invalid_configuration("workbook locator is missing its allowed-root label")
    })?;
    let value = label.strip_prefix("root-").ok_or_else(|| {
        ConnectorError::invalid_configuration("workbook locator has an invalid allowed-root label")
    })?;
    let index = value.parse::<usize>().map_err(|_| {
        ConnectorError::invalid_configuration("workbook locator has an invalid allowed-root label")
    })?;
    if root_label(index) != label {
        return Err(ConnectorError::invalid_configuration(
            "workbook locator has a non-canonical allowed-root label",
        ));
    }
    Ok(index)
}

pub(crate) fn root_label(index: usize) -> String {
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
    category: ErrorCategory,
    retryable: bool,
    message: &'static str,
) -> ConnectorError {
    ConnectorError::with_category(
        category,
        retryable,
        message,
        Vec::new(),
        std::collections::BTreeMap::new(),
    )
}

fn path_open_error(error: std::io::Error, message: &'static str) -> ConnectorError {
    let category = if error.kind() == std::io::ErrorKind::NotFound {
        ErrorCategory::NotFound
    } else {
        ErrorCategory::Authorization
    };
    source_error(category, false, message)
}
