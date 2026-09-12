//! The closed, deterministic node registry (NX-N1, #337; frozen by the
//! NX-C0 contract §3/§6). The registry performs stable lookup and ordering
//! only: it never writes a node's rules, and duplicate
//! `(typeId, configVersion)` registrations fail closed.

use std::collections::BTreeMap;

use super::composite::NodePackage;
use super::definition::NodeDefinition;
use super::{NodeCatalogEntry, NodeGraphError, NodeGraphErrorCode};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeRegistry {
    definitions: Vec<NodeDefinition>,
}

impl Default for NodeRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl NodeRegistry {
    /// The production registry: the closed built-in definition list, sorted
    /// by `typeId` then `configVersion`.
    pub fn new() -> Self {
        let definitions = super::definitions::production_definitions();
        // The production list is static and duplicate-free; the mechanical
        // guard below keeps that true without a fallible public signature.
        let registry =
            Self::from_definitions(definitions).expect("production definitions are unique");
        debug_assert_eq!(registry.definitions.len(), 11);
        registry
    }

    /// Registers a closed definition set. Registration order never matters:
    /// the catalog and lookup results are sorted by `typeId` then
    /// `configVersion`, and a duplicate `(typeId, configVersion)` pair is
    /// rejected strictly.
    pub fn from_definitions(definitions: Vec<NodeDefinition>) -> Result<Self, NodeGraphError> {
        let mut definitions = definitions;
        definitions.sort_by(|left, right| {
            left.type_id()
                .cmp(right.type_id())
                .then(left.config_version().cmp(&right.config_version()))
        });
        for pair in definitions.windows(2) {
            if pair[0].type_id() == pair[1].type_id()
                && pair[0].config_version() == pair[1].config_version()
            {
                return Err(NodeGraphError::new(
                    NodeGraphErrorCode::InvalidConfig,
                    None,
                    format!(
                        "node type {} is registered more than once for config version {}",
                        pair[0].type_id(),
                        pair[0].config_version()
                    ),
                ));
            }
        }
        Ok(Self { definitions })
    }

    pub fn definitions(&self) -> &[NodeDefinition] {
        &self.definitions
    }

    /// Deploys declarative node packages on top of the current registry
    /// (NX-C1 §6.2): explicit, additive, sorted, duplicate-refusing. The
    /// production registry stays atomic-only; deployment is an operator
    /// decision over audited packages.
    pub fn with_packages(mut self, packages: Vec<NodePackage>) -> Result<Self, NodeGraphError> {
        let mut definitions = Vec::with_capacity(packages.len());
        let mut seen_versions: BTreeMap<(String, String, String), String> = BTreeMap::new();
        for package in &packages {
            super::composite::validate_package(package)?;
            let key = (
                package.namespace.clone(),
                package.name.clone(),
                package.version.clone(),
            );
            if let Some(existing) = seen_versions.get(&key) {
                if existing != &package.content_digest {
                    return Err(NodeGraphError::new(
                        NodeGraphErrorCode::InvalidConfig,
                        None,
                        format!(
                            "package content changed for {}/{}/{}@{}",
                            package.namespace, package.name, package.version, existing
                        ),
                    ));
                }
            }
            seen_versions.insert(key, package.content_digest.clone());
            definitions.push(super::composite::package_definition(package));
        }
        self.definitions.extend(definitions);
        self.definitions.sort_by(|left, right| {
            left.type_id()
                .cmp(right.type_id())
                .then(left.config_version().cmp(&right.config_version()))
        });
        for pair in self.definitions.windows(2) {
            if pair[0].type_id() == pair[1].type_id()
                && pair[0].config_version() == pair[1].config_version()
            {
                return Err(NodeGraphError::new(
                    NodeGraphErrorCode::InvalidConfig,
                    None,
                    format!(
                        "node type {} is registered more than once for config version {}",
                        pair[0].type_id(),
                        pair[0].config_version()
                    ),
                ));
            }
        }
        Ok(self)
    }

    /// The production registry with the deployed package manifest applied.
    /// The static manifest is test-verified; a failure here is a build-time
    /// defect, not a runtime condition.
    pub fn deployed() -> Self {
        Self::new()
            .with_packages(super::composite::deployed_packages())
            .expect("deployed packages are valid")
    }

    pub fn catalog(&self) -> Vec<NodeCatalogEntry> {
        self.definitions
            .iter()
            .map(NodeDefinition::catalog_entry)
            .collect()
    }

    pub fn lookup(
        &self,
        type_id: &str,
        config_version: u16,
    ) -> Result<&NodeDefinition, NodeGraphError> {
        let type_exists = self
            .definitions
            .iter()
            .any(|definition| definition.type_id() == type_id);
        self.definitions
            .binary_search_by(|definition| {
                definition
                    .type_id()
                    .cmp(type_id)
                    .then(definition.config_version().cmp(&config_version))
            })
            .map(|index| &self.definitions[index])
            .map_err(|_| {
                if type_exists {
                    NodeGraphError::new(
                        NodeGraphErrorCode::UnsupportedConfigVersion,
                        None,
                        "node config version is not supported",
                    )
                } else {
                    NodeGraphError::new(
                        NodeGraphErrorCode::UnknownNodeType,
                        None,
                        "node type is not registered",
                    )
                }
            })
    }
}
