use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;
use uuid::Uuid;

use crate::{ensure_no_secret_fields, ColumnId, Expr, LogicalType, ScalarValue};

pub(crate) mod definition;
pub(crate) mod definitions;
mod registry;

pub use definition::{
    ConfigConstraints, ConfigField, ConfigSchema, ConfigValueKind, NodeCatalogEntry,
    NodeDefinition, NodeLoweringTarget, NodeRole, NodeSupportStatus, PortSupportConditions,
    SupportCondition,
};
pub use registry::NodeRegistry;

pub const NODE_GRAPH_VERSION: u16 = 1;
pub const MAX_GRAPH_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_NODES: usize = 64;
pub const MAX_EDGES: usize = MAX_NODES - 1;
pub const MAX_CONFIG_BYTES: usize = 64 * 1024;
pub const MAX_TOTAL_CONFIG_BYTES: usize = 1024 * 1024;
pub const MAX_STRING_BYTES: usize = 4 * 1024;
pub const MAX_METADATA_BYTES: usize = 64 * 1024;
pub const MAX_NESTING_DEPTH: usize = 64;
pub const MAX_EXPR_NODES: usize = 1_024;
pub const MAX_EXPR_DEPTH: usize = 64;
pub const MAX_RULES_PER_NODE: usize = 1;
pub const MAX_RULES: usize = MAX_NODES;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeGraphErrorCode {
    UnsupportedGraphVersion,
    UnknownNodeType,
    UnsupportedConfigVersion,
    InvalidConfig,
    InvalidPort,
    InvalidTopology,
    SourceBinding,
    UnknownColumn,
    IncompatibleType,
    UnsupportedTarget,
    LimitGraphBytes,
    LimitNodes,
    LimitEdges,
    LimitConfigBytes,
    LimitMetadataBytes,
    LimitStringBytes,
    LimitNestingDepth,
    LimitRulesPerNode,
    LimitRules,
    LimitCompileWork,
    PlanInvalid,
    Internal,
}

impl NodeGraphErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UnsupportedGraphVersion => "NG_UNSUPPORTED_GRAPH_VERSION",
            Self::UnknownNodeType => "NG_UNKNOWN_NODE_TYPE",
            Self::UnsupportedConfigVersion => "NG_UNSUPPORTED_CONFIG_VERSION",
            Self::InvalidConfig => "NG_INVALID_CONFIG",
            Self::InvalidPort => "NG_INVALID_PORT",
            Self::InvalidTopology => "NG_INVALID_TOPOLOGY",
            Self::SourceBinding => "NG_SOURCE_BINDING",
            Self::UnknownColumn => "NG_UNKNOWN_COLUMN",
            Self::IncompatibleType => "NG_INCOMPATIBLE_TYPE",
            Self::UnsupportedTarget => "NG_UNSUPPORTED_TARGET",
            Self::LimitGraphBytes => "NG_LIMIT_GRAPH_BYTES",
            Self::LimitNodes => "NG_LIMIT_NODES",
            Self::LimitEdges => "NG_LIMIT_EDGES",
            Self::LimitConfigBytes => "NG_LIMIT_CONFIG_BYTES",
            Self::LimitMetadataBytes => "NG_LIMIT_METADATA_BYTES",
            Self::LimitStringBytes => "NG_LIMIT_STRING_BYTES",
            Self::LimitNestingDepth => "NG_LIMIT_NESTING_DEPTH",
            Self::LimitRulesPerNode => "NG_LIMIT_RULES_PER_NODE",
            Self::LimitRules => "NG_LIMIT_RULES",
            Self::LimitCompileWork => "NG_LIMIT_COMPILE_WORK",
            Self::PlanInvalid => "NG_PLAN_INVALID",
            Self::Internal => "NG_INTERNAL",
        }
    }
}

impl fmt::Display for NodeGraphErrorCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{code}: {message}")]
pub struct NodeGraphError {
    code: NodeGraphErrorCode,
    node_id: Option<NodeId>,
    field_path: Option<String>,
    message: String,
}

impl NodeGraphError {
    fn new(code: NodeGraphErrorCode, node_id: Option<NodeId>, message: impl Into<String>) -> Self {
        Self {
            code,
            node_id,
            field_path: None,
            message: message.into(),
        }
    }

    /// Attaches the bounded config field path this failure is attributed to
    /// (NX-C0 §7.1: structural paths only, never caller values).
    fn with_field_path(mut self, field: impl Into<String>) -> Self {
        self.field_path = Some(field.into());
        self
    }

    pub const fn code(&self) -> NodeGraphErrorCode {
        self.code
    }

    pub const fn node_id(&self) -> Option<NodeId> {
        self.node_id
    }

    pub fn field_path(&self) -> Option<&str> {
        self.field_path.as_deref()
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct NodeId(Uuid);

impl NodeId {
    pub const fn from_uuid(value: Uuid) -> Self {
        Self(value)
    }

    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl<'de> Deserialize<'de> for NodeId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Uuid::deserialize(deserializer)?;
        if value.is_nil() {
            return Err(serde::de::Error::custom("node id must not be nil"));
        }
        Ok(Self(value))
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct PortId(String);

impl PortId {
    pub fn new(value: impl Into<String>) -> Result<Self, NodeGraphError> {
        let value = value.into();
        validate_string(&value, None, "port id")?;
        if value.is_empty() || !value.is_ascii() {
            return Err(NodeGraphError::new(
                NodeGraphErrorCode::InvalidPort,
                None,
                "port id must be non-empty ASCII",
            ));
        }
        Ok(Self(value))
    }

    fn from_static(value: &'static str) -> Self {
        Self(value.to_owned())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for PortId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

impl fmt::Display for PortId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NodePort {
    pub node_id: NodeId,
    pub port: PortId,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NodeEdge {
    pub from: NodePort,
    pub to: NodePort,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeConfig {
    pub id: NodeId,
    pub type_id: String,
    pub config_version: u16,
    pub config: Value,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct NodeConfigData {
    id: NodeId,
    type_id: String,
    config_version: u16,
    config: Value,
    #[serde(default)]
    metadata: BTreeMap<String, String>,
}

impl<'de> Deserialize<'de> for NodeConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let data = NodeConfigData::deserialize(deserializer)?;
        Self::from_parts(
            data.id,
            data.type_id,
            data.config_version,
            data.config,
            data.metadata,
        )
        .map_err(serde::de::Error::custom)
    }
}

impl NodeConfig {
    pub fn new(
        id: NodeId,
        type_id: impl Into<String>,
        config_version: u16,
        config: Value,
        metadata: BTreeMap<String, String>,
    ) -> Result<Self, NodeGraphError> {
        Self::from_parts(id, type_id, config_version, config, metadata)
    }

    fn from_parts(
        id: NodeId,
        type_id: impl Into<String>,
        config_version: u16,
        config: Value,
        metadata: BTreeMap<String, String>,
    ) -> Result<Self, NodeGraphError> {
        let node = Self {
            id,
            type_id: type_id.into(),
            config_version,
            config,
            metadata,
        };
        node.validate_local()?;
        Ok(node)
    }

    fn validate_local(&self) -> Result<usize, NodeGraphError> {
        if self.id.as_uuid().is_nil() {
            return Err(NodeGraphError::new(
                NodeGraphErrorCode::InvalidConfig,
                Some(self.id),
                "node id must not be nil",
            ));
        }
        validate_string(&self.type_id, Some(self.id), "type id")?;
        if self.type_id.is_empty() {
            return Err(NodeGraphError::new(
                NodeGraphErrorCode::InvalidConfig,
                Some(self.id),
                "type id must not be empty",
            ));
        }
        if !self.config.is_object() {
            return Err(NodeGraphError::new(
                NodeGraphErrorCode::InvalidConfig,
                Some(self.id),
                "node config must be a JSON object",
            ));
        }
        let config_bytes = serde_json::to_vec(&self.config).map_err(|_| {
            NodeGraphError::new(
                NodeGraphErrorCode::InvalidConfig,
                Some(self.id),
                "node config could not be serialized",
            )
        })?;
        if config_bytes.len() > MAX_CONFIG_BYTES {
            return Err(NodeGraphError::new(
                NodeGraphErrorCode::LimitConfigBytes,
                Some(self.id),
                format!("node config exceeds {} bytes", MAX_CONFIG_BYTES),
            ));
        }
        validate_json_limits(&self.config, Some(self.id))?;
        ensure_no_secret_fields(&self.config).map_err(|_| {
            NodeGraphError::new(
                NodeGraphErrorCode::InvalidConfig,
                Some(self.id),
                "node config contains secret-like data",
            )
        })?;
        validate_metadata(&self.metadata, Some(self.id))?;
        Ok(config_bytes.len())
    }

    pub fn id(&self) -> NodeId {
        self.id
    }

    pub fn type_id(&self) -> &str {
        &self.type_id
    }

    pub const fn config_version(&self) -> u16 {
        self.config_version
    }

    pub fn config(&self) -> &Value {
        &self.config
    }

    pub fn metadata(&self) -> &BTreeMap<String, String> {
        &self.metadata
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeGraph {
    pub version: u16,
    pub graph_id: Uuid,
    pub source_node_id: NodeId,
    pub output_node_id: NodeId,
    pub nodes: Vec<NodeConfig>,
    pub edges: Vec<NodeEdge>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct NodeGraphData {
    version: u16,
    graph_id: Uuid,
    source_node_id: NodeId,
    output_node_id: NodeId,
    nodes: Vec<NodeConfig>,
    edges: Vec<NodeEdge>,
    #[serde(default)]
    metadata: BTreeMap<String, String>,
}

impl<'de> Deserialize<'de> for NodeGraph {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let data = NodeGraphData::deserialize(deserializer)?;
        Self::from_parts(
            data.version,
            data.graph_id,
            data.source_node_id,
            data.output_node_id,
            data.nodes,
            data.edges,
            data.metadata,
        )
        .map_err(serde::de::Error::custom)
    }
}

impl NodeGraph {
    pub fn new(
        graph_id: Uuid,
        source_node_id: NodeId,
        output_node_id: NodeId,
        nodes: Vec<NodeConfig>,
        edges: Vec<NodeEdge>,
        metadata: BTreeMap<String, String>,
    ) -> Result<Self, NodeGraphError> {
        Self::from_parts(
            NODE_GRAPH_VERSION,
            graph_id,
            source_node_id,
            output_node_id,
            nodes,
            edges,
            metadata,
        )
    }

    fn from_parts(
        version: u16,
        graph_id: Uuid,
        source_node_id: NodeId,
        output_node_id: NodeId,
        nodes: Vec<NodeConfig>,
        edges: Vec<NodeEdge>,
        metadata: BTreeMap<String, String>,
    ) -> Result<Self, NodeGraphError> {
        let graph = Self {
            version,
            graph_id,
            source_node_id,
            output_node_id,
            nodes,
            edges,
            metadata,
        };
        graph.validate_basic()?;
        Ok(graph)
    }

    pub fn from_json_bytes(bytes: &[u8], registry: &NodeRegistry) -> Result<Self, NodeGraphError> {
        if bytes.len() > MAX_GRAPH_BYTES {
            return Err(NodeGraphError::new(
                NodeGraphErrorCode::LimitGraphBytes,
                None,
                format!("serialized graph exceeds {} bytes", MAX_GRAPH_BYTES),
            ));
        }
        let data: NodeGraphData = serde_json::from_slice(bytes).map_err(|_| {
            NodeGraphError::new(
                NodeGraphErrorCode::InvalidConfig,
                None,
                "serialized graph is invalid",
            )
        })?;
        let graph = Self::from_parts(
            data.version,
            data.graph_id,
            data.source_node_id,
            data.output_node_id,
            data.nodes,
            data.edges,
            data.metadata,
        )?;
        graph.validate(registry)?;
        Ok(graph)
    }

    pub fn validate(&self, registry: &NodeRegistry) -> Result<(), NodeGraphError> {
        self.validated_configs(registry).map(|_| ())
    }

    pub fn validated_configs(
        &self,
        registry: &NodeRegistry,
    ) -> Result<BTreeMap<NodeId, ValidatedNodeConfig>, NodeGraphError> {
        self.validate_basic()?;
        if self.nodes.len() < 2 {
            return Err(NodeGraphError::new(
                NodeGraphErrorCode::InvalidTopology,
                None,
                "graph must contain a source and output",
            ));
        }

        let mut definitions = BTreeMap::new();
        let mut configs = BTreeMap::new();
        let mut source_count = 0_usize;
        let mut output_count = 0_usize;
        // Intra-stage fault order is canonical (NX-C0 §7.2, R-8): nodes are
        // validated by ascending NodeId, never by caller array order.
        let mut ordered_nodes: Vec<&NodeConfig> = self.nodes.iter().collect();
        ordered_nodes.sort_by_key(|node| node.id());
        for node in ordered_nodes {
            let definition = registry
                .lookup(node.type_id(), node.config_version())
                .map_err(|error| {
                    NodeGraphError::new(error.code(), Some(node.id()), error.message())
                })?;
            let typed = definition.validate_node_config(node)?;
            if definition.is_source() {
                source_count += 1;
            }
            if definition.is_output() {
                output_count += 1;
            }
            definitions.insert(node.id(), definition);
            configs.insert(node.id(), typed);
        }

        if source_count != 1 || output_count != 1 {
            return Err(NodeGraphError::new(
                NodeGraphErrorCode::InvalidTopology,
                None,
                "graph must contain exactly one source and one output",
            ));
        }
        let source_definition = definitions.get(&self.source_node_id).ok_or_else(|| {
            NodeGraphError::new(
                NodeGraphErrorCode::InvalidTopology,
                Some(self.source_node_id),
                "declared source node is absent",
            )
        })?;
        if !source_definition.is_source() {
            return Err(NodeGraphError::new(
                NodeGraphErrorCode::InvalidTopology,
                Some(self.source_node_id),
                "declared source node is not a source",
            ));
        }
        let output_definition = definitions.get(&self.output_node_id).ok_or_else(|| {
            NodeGraphError::new(
                NodeGraphErrorCode::InvalidTopology,
                Some(self.output_node_id),
                "declared output node is absent",
            )
        })?;
        if !output_definition.is_output() {
            return Err(NodeGraphError::new(
                NodeGraphErrorCode::InvalidTopology,
                Some(self.output_node_id),
                "declared output node is not an output",
            ));
        }

        let mut incoming: BTreeMap<NodeId, Vec<&NodeEdge>> = self
            .nodes
            .iter()
            .map(|node| (node.id(), Vec::new()))
            .collect();
        let mut outgoing: BTreeMap<NodeId, Vec<&NodeEdge>> = self
            .nodes
            .iter()
            .map(|node| (node.id(), Vec::new()))
            .collect();
        for edge in &self.edges {
            let from_definition = definitions.get(&edge.from.node_id).ok_or_else(|| {
                NodeGraphError::new(
                    NodeGraphErrorCode::InvalidTopology,
                    Some(edge.from.node_id),
                    "edge source node is absent",
                )
            })?;
            let to_definition = definitions.get(&edge.to.node_id).ok_or_else(|| {
                NodeGraphError::new(
                    NodeGraphErrorCode::InvalidTopology,
                    Some(edge.to.node_id),
                    "edge target node is absent",
                )
            })?;
            if !from_definition.output_ports().contains(&edge.from.port) {
                return Err(NodeGraphError::new(
                    NodeGraphErrorCode::InvalidPort,
                    Some(edge.from.node_id),
                    "edge source port is not declared",
                ));
            }
            if !to_definition.input_ports().contains(&edge.to.port) {
                return Err(NodeGraphError::new(
                    NodeGraphErrorCode::InvalidPort,
                    Some(edge.to.node_id),
                    "edge target port is not declared",
                ));
            }
            if edge.from.node_id == edge.to.node_id {
                return Err(NodeGraphError::new(
                    NodeGraphErrorCode::InvalidTopology,
                    Some(edge.from.node_id),
                    "self-edge is not supported",
                ));
            }
            outgoing
                .get_mut(&edge.from.node_id)
                .ok_or_else(|| {
                    NodeGraphError::new(
                        NodeGraphErrorCode::Internal,
                        Some(edge.from.node_id),
                        "resolved outgoing node is absent",
                    )
                })?
                .push(edge);
            incoming
                .get_mut(&edge.to.node_id)
                .ok_or_else(|| {
                    NodeGraphError::new(
                        NodeGraphErrorCode::Internal,
                        Some(edge.to.node_id),
                        "resolved incoming node is absent",
                    )
                })?
                .push(edge);
        }

        for node in &self.nodes {
            let definition = definitions.get(&node.id()).ok_or_else(|| {
                NodeGraphError::new(
                    NodeGraphErrorCode::Internal,
                    Some(node.id()),
                    "resolved node definition is absent",
                )
            })?;
            let in_count = incoming
                .get(&node.id())
                .ok_or_else(|| {
                    NodeGraphError::new(
                        NodeGraphErrorCode::Internal,
                        Some(node.id()),
                        "resolved incoming node is absent",
                    )
                })?
                .len();
            let out_count = outgoing
                .get(&node.id())
                .ok_or_else(|| {
                    NodeGraphError::new(
                        NodeGraphErrorCode::Internal,
                        Some(node.id()),
                        "resolved outgoing node is absent",
                    )
                })?
                .len();
            if (definition.is_source() && in_count != 0)
                || (!definition.is_source() && in_count != 1)
                || (definition.is_output() && out_count != 0)
                || (!definition.is_output() && out_count != 1)
            {
                return Err(NodeGraphError::new(
                    NodeGraphErrorCode::InvalidTopology,
                    Some(node.id()),
                    "Phase-1 graph must be one connected linear path",
                ));
            }
        }

        let mut visited = BTreeSet::new();
        let mut current = self.source_node_id;
        loop {
            if !visited.insert(current) {
                return Err(NodeGraphError::new(
                    NodeGraphErrorCode::InvalidTopology,
                    Some(current),
                    "graph contains a cycle",
                ));
            }
            if current == self.output_node_id {
                break;
            }
            let next = outgoing
                .get(&current)
                .and_then(|edges| edges.first())
                .map(|edge| edge.to.node_id)
                .ok_or_else(|| {
                    NodeGraphError::new(
                        NodeGraphErrorCode::InvalidTopology,
                        Some(current),
                        "path terminates before output",
                    )
                })?;
            current = next;
        }
        if visited.len() != self.nodes.len() {
            return Err(NodeGraphError::new(
                NodeGraphErrorCode::InvalidTopology,
                None,
                "graph contains disconnected nodes or an alternate path",
            ));
        }
        Ok(configs)
    }

    pub fn canonical_bytes(&self) -> Result<Vec<u8>, NodeGraphError> {
        self.validate_basic()?;
        let mut normalized = self.clone();
        normalized.nodes.sort_by_key(NodeConfig::id);
        normalized.edges.sort();
        for node in &mut normalized.nodes {
            node.config = canonicalize_json(&node.config);
        }
        serde_json::to_vec(&normalized).map_err(|_| {
            NodeGraphError::new(
                NodeGraphErrorCode::Internal,
                None,
                "graph canonicalization failed",
            )
        })
    }

    fn validate_basic(&self) -> Result<usize, NodeGraphError> {
        if self.version != NODE_GRAPH_VERSION {
            return Err(NodeGraphError::new(
                NodeGraphErrorCode::UnsupportedGraphVersion,
                None,
                format!("graph version {} is not supported", self.version),
            ));
        }
        if self.graph_id.is_nil() {
            return Err(NodeGraphError::new(
                NodeGraphErrorCode::InvalidConfig,
                None,
                "graph id must not be nil",
            ));
        }
        if self.source_node_id == self.output_node_id {
            return Err(NodeGraphError::new(
                NodeGraphErrorCode::InvalidTopology,
                Some(self.source_node_id),
                "source and output must be different nodes",
            ));
        }
        if self.nodes.len() > MAX_NODES {
            return Err(NodeGraphError::new(
                NodeGraphErrorCode::LimitNodes,
                None,
                format!("graph contains more than {} nodes", MAX_NODES),
            ));
        }
        if self.edges.len() > MAX_EDGES {
            return Err(NodeGraphError::new(
                NodeGraphErrorCode::LimitEdges,
                None,
                format!("graph contains more than {} edges", MAX_EDGES),
            ));
        }
        let expected_edges = self.nodes.len().checked_sub(1).ok_or_else(|| {
            NodeGraphError::new(
                NodeGraphErrorCode::InvalidTopology,
                None,
                "graph must contain nodes",
            )
        })?;
        if self.edges.len() != expected_edges {
            return Err(NodeGraphError::new(
                NodeGraphErrorCode::InvalidTopology,
                None,
                "Phase-1 graph must have exactly nodes minus one edges",
            ));
        }
        let mut node_ids = BTreeSet::new();
        let mut total_config_bytes = 0_usize;
        for node in &self.nodes {
            if !node_ids.insert(node.id()) {
                return Err(NodeGraphError::new(
                    NodeGraphErrorCode::InvalidTopology,
                    Some(node.id()),
                    "node ids must be unique",
                ));
            }
            total_config_bytes = total_config_bytes
                .checked_add(node.validate_local()?)
                .ok_or_else(|| {
                    NodeGraphError::new(
                        NodeGraphErrorCode::LimitConfigBytes,
                        Some(node.id()),
                        "aggregate config size overflowed",
                    )
                })?;
            if total_config_bytes > MAX_TOTAL_CONFIG_BYTES {
                return Err(NodeGraphError::new(
                    NodeGraphErrorCode::LimitConfigBytes,
                    Some(node.id()),
                    format!("aggregate config exceeds {} bytes", MAX_TOTAL_CONFIG_BYTES),
                ));
            }
        }
        if !node_ids.contains(&self.source_node_id) || !node_ids.contains(&self.output_node_id) {
            return Err(NodeGraphError::new(
                NodeGraphErrorCode::InvalidTopology,
                None,
                "source and output nodes must be present",
            ));
        }
        let mut edge_ids = BTreeSet::new();
        for edge in &self.edges {
            if !edge_ids.insert(edge.clone()) {
                return Err(NodeGraphError::new(
                    NodeGraphErrorCode::InvalidTopology,
                    Some(edge.from.node_id),
                    "edge tuples must be unique",
                ));
            }
            if !node_ids.contains(&edge.from.node_id) || !node_ids.contains(&edge.to.node_id) {
                return Err(NodeGraphError::new(
                    NodeGraphErrorCode::InvalidTopology,
                    None,
                    "edge endpoints must reference existing nodes",
                ));
            }
        }
        let metadata_bytes = validate_metadata(&self.metadata, None)?;
        let total_metadata_bytes = self.nodes.iter().try_fold(metadata_bytes, |total, node| {
            let node_bytes = validate_metadata(&node.metadata, Some(node.id()))?;
            total.checked_add(node_bytes).ok_or_else(|| {
                NodeGraphError::new(
                    NodeGraphErrorCode::LimitMetadataBytes,
                    Some(node.id()),
                    "aggregate metadata size overflowed",
                )
            })
        })?;
        if total_metadata_bytes > MAX_METADATA_BYTES {
            return Err(NodeGraphError::new(
                NodeGraphErrorCode::LimitMetadataBytes,
                None,
                format!("aggregate metadata exceeds {} bytes", MAX_METADATA_BYTES),
            ));
        }
        let serialized = serde_json::to_vec(self).map_err(|_| {
            NodeGraphError::new(
                NodeGraphErrorCode::Internal,
                None,
                "graph serialization failed",
            )
        })?;
        if serialized.len() > MAX_GRAPH_BYTES {
            return Err(NodeGraphError::new(
                NodeGraphErrorCode::LimitGraphBytes,
                None,
                format!("serialized graph exceeds {} bytes", MAX_GRAPH_BYTES),
            ));
        }
        Ok(total_config_bytes)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidatedNodeConfig {
    Source {
        source_asset_id: Uuid,
        projection: Option<Vec<ColumnId>>,
    },
    Select {
        columns: Vec<ColumnId>,
    },
    Filter {
        predicate: Expr,
    },
    Rename {
        column: ColumnId,
        to: String,
    },
    Trim {
        column: ColumnId,
    },
    Cast {
        column: ColumnId,
        data_type: LogicalType,
        on_failure: CastFailurePolicy,
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
    Output {
        output_label: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CastFailurePolicy {
    Error,
    SetNull,
}

fn validate_string(
    value: &str,
    node_id: Option<NodeId>,
    field: &str,
) -> Result<(), NodeGraphError> {
    if value.len() > MAX_STRING_BYTES {
        return Err(NodeGraphError::new(
            NodeGraphErrorCode::LimitStringBytes,
            node_id,
            format!("{field} exceeds {} UTF-8 bytes", MAX_STRING_BYTES),
        ));
    }
    Ok(())
}

fn validate_metadata(
    metadata: &BTreeMap<String, String>,
    node_id: Option<NodeId>,
) -> Result<usize, NodeGraphError> {
    let mut bytes = 0_usize;
    for (key, value) in metadata {
        validate_string(key, node_id, "metadata key")?;
        validate_string(value, node_id, "metadata value")?;
        bytes = bytes
            .checked_add(key.len())
            .and_then(|total| total.checked_add(value.len()))
            .ok_or_else(|| {
                NodeGraphError::new(
                    NodeGraphErrorCode::LimitMetadataBytes,
                    node_id,
                    "metadata size overflowed",
                )
            })?;
    }
    let json = serde_json::to_value(metadata).map_err(|_| {
        NodeGraphError::new(
            NodeGraphErrorCode::InvalidConfig,
            node_id,
            "metadata could not be serialized",
        )
    })?;
    ensure_no_secret_fields(&json).map_err(|_| {
        NodeGraphError::new(
            NodeGraphErrorCode::InvalidConfig,
            node_id,
            "metadata contains secret-like data",
        )
    })?;
    Ok(bytes)
}

fn validate_json_limits(value: &Value, node_id: Option<NodeId>) -> Result<(), NodeGraphError> {
    let mut pending = vec![(value, 1_usize)];
    while let Some((current, depth)) = pending.pop() {
        if depth > MAX_NESTING_DEPTH {
            return Err(NodeGraphError::new(
                NodeGraphErrorCode::LimitNestingDepth,
                node_id,
                "JSON nesting depth bound exceeded",
            ));
        }
        match current {
            Value::Object(object) => {
                for (key, item) in object {
                    validate_string(key, node_id, "config key")?;
                    pending.push((item, depth + 1));
                }
            }
            Value::Array(items) => {
                pending.extend(items.iter().map(|item| (item, depth + 1)));
            }
            Value::String(string) => validate_string(string, node_id, "config string")?,
            Value::Null | Value::Bool(_) | Value::Number(_) => {}
        }
    }
    Ok(())
}

fn canonicalize_json(value: &Value) -> Value {
    match value {
        Value::Object(object) => {
            let mut normalized = Map::new();
            let mut entries: Vec<_> = object.iter().collect();
            entries.sort_by(|left, right| left.0.cmp(right.0));
            for (key, value) in entries {
                normalized.insert(key.clone(), canonicalize_json(value));
            }
            Value::Object(normalized)
        }
        Value::Array(items) => Value::Array(items.iter().map(canonicalize_json).collect()),
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;
    use uuid::Uuid;

    use super::*;

    fn id(value: u128) -> NodeId {
        NodeId::from_uuid(Uuid::from_u128(value))
    }

    fn column(value: u128) -> String {
        Uuid::from_u128(value).to_string()
    }

    fn node(value: u128, type_id: &str, config: Value) -> NodeConfig {
        NodeConfig::new(id(value), type_id, 1, config, BTreeMap::new()).expect("node")
    }

    fn edge(from: u128, to: u128) -> NodeEdge {
        NodeEdge {
            from: NodePort {
                node_id: id(from),
                port: PortId::new("out").expect("port"),
            },
            to: NodePort {
                node_id: id(to),
                port: PortId::new("in").expect("port"),
            },
        }
    }

    fn graph() -> NodeGraph {
        let source = node(
            1,
            "stillflow.node.source",
            json!({"sourceAssetId": Uuid::from_u128(101)}),
        );
        let select = node(
            2,
            "stillflow.node.select",
            json!({"columns": [column(201)]}),
        );
        let output = node(3, "stillflow.node.output", json!({"outputLabel": "clean"}));
        NodeGraph::new(
            Uuid::from_u128(301),
            id(1),
            id(3),
            vec![source, select, output],
            vec![edge(1, 2), edge(2, 3)],
            BTreeMap::new(),
        )
        .expect("graph")
    }

    #[test]
    fn graph_roundtrips_at_frozen_version_and_canonicalizes_order() {
        let graph = graph();
        graph.validate(&NodeRegistry::new()).expect("valid graph");
        let encoded = serde_json::to_vec(&graph).expect("encode");
        let restored: NodeGraph = serde_json::from_slice(&encoded).expect("decode");
        assert_eq!(serde_json::to_vec(&restored).expect("encode"), encoded);
        assert_eq!(
            graph.canonical_bytes().expect("canonical"),
            restored.canonical_bytes().expect("canonical")
        );
    }

    #[test]
    fn rejects_unknown_versions_types_and_config_fields() {
        let mut value = serde_json::to_value(graph()).expect("value");
        value["version"] = json!(2);
        let error = serde_json::from_value::<NodeGraph>(value).expect_err("version");
        assert!(error.to_string().contains("NG_UNSUPPORTED_GRAPH_VERSION"));

        let bytes = serde_json::to_vec(&graph()).expect("encode");
        let mut versioned = serde_json::from_slice::<Value>(&bytes).expect("value");
        versioned["version"] = json!(2);
        let error = NodeGraph::from_json_bytes(
            &serde_json::to_vec(&versioned).expect("encode"),
            &NodeRegistry::new(),
        )
        .expect_err("version");
        assert_eq!(error.code(), NodeGraphErrorCode::UnsupportedGraphVersion);

        let mut unknown = graph();
        unknown.nodes[1].type_id = "stillflow.node.unknown".to_owned();
        let error = unknown.validate(&NodeRegistry::new()).expect_err("type");
        assert_eq!(error.code(), NodeGraphErrorCode::UnknownNodeType);

        let mut extra = graph();
        extra.nodes[1].config = json!({"columns": [column(201)], "extra": true});
        let error = extra
            .validate(&NodeRegistry::new())
            .expect_err("unknown field");
        assert_eq!(error.code(), NodeGraphErrorCode::InvalidConfig);

        let mut wrong_version = graph();
        wrong_version.nodes[1].config_version = 2;
        let error = wrong_version
            .validate(&NodeRegistry::new())
            .expect_err("config version");
        assert_eq!(error.code(), NodeGraphErrorCode::UnsupportedConfigVersion);
    }

    #[test]
    fn enforces_linear_topology_and_ports() {
        let mut branched = graph();
        branched.edges[0].from.node_id = id(1);
        branched.edges[0].to.node_id = id(3);
        let error = branched
            .validate(&NodeRegistry::new())
            .expect_err("wrong edge");
        assert_eq!(error.code(), NodeGraphErrorCode::InvalidTopology);

        let mut disconnected = graph();
        disconnected.nodes[1].id = id(4);
        let error = disconnected
            .validate(&NodeRegistry::new())
            .expect_err("missing edge endpoint");
        assert_eq!(error.code(), NodeGraphErrorCode::InvalidTopology);
    }

    #[test]
    fn registry_is_closed_sorted_and_catalog_is_stable() {
        let first = NodeRegistry::new();
        let second = NodeRegistry::new();
        let first_ids: Vec<_> = first
            .definitions()
            .iter()
            .map(|definition| definition.type_id())
            .collect();
        let second_ids: Vec<_> = second
            .definitions()
            .iter()
            .map(|definition| definition.type_id())
            .collect();
        assert_eq!(first_ids, second_ids);
        assert!(first_ids.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(first.catalog(), second.catalog());
        assert_eq!(first.catalog().len(), 11);
        assert_eq!(
            first.lookup("stillflow.node.source", 2).unwrap_err().code(),
            NodeGraphErrorCode::UnsupportedConfigVersion
        );
        assert_eq!(
            first
                .lookup("stillflow.node.unknown", 1)
                .unwrap_err()
                .code(),
            NodeGraphErrorCode::UnknownNodeType
        );
    }

    #[test]
    fn rejects_unknown_fields_at_every_envelope_level() {
        let base = serde_json::to_value(graph()).expect("value");

        // graph level
        let mut value = base.clone();
        value["extra"] = json!(true);
        let error = serde_json::from_value::<NodeGraph>(value).expect_err("graph level");
        assert!(error.to_string().contains("unknown field"), "{error}");

        // node level
        let mut value = base.clone();
        value["nodes"][0]["extra"] = json!(true);
        let error = serde_json::from_value::<NodeGraph>(value).expect_err("node level");
        assert!(error.to_string().contains("unknown field"), "{error}");

        // edge level
        let mut value = base.clone();
        value["edges"][0]["extra"] = json!(true);
        let error = serde_json::from_value::<NodeGraph>(value).expect_err("edge level");
        assert!(error.to_string().contains("unknown field"), "{error}");

        // port level
        let mut value = base.clone();
        value["edges"][0]["from"]["extra"] = json!(true);
        let error = serde_json::from_value::<NodeGraph>(value).expect_err("port level");
        assert!(error.to_string().contains("unknown field"), "{error}");

        // A graph without extra keys still decodes.
        assert!(serde_json::from_value::<NodeGraph>(base).is_ok());
    }

    #[test]
    fn rejects_secret_config_and_frozen_bounds() {
        let secret = NodeConfig::new(
            id(1),
            "stillflow.node.output",
            1,
            json!({"outputLabel": "token=hidden"}),
            BTreeMap::new(),
        )
        .expect_err("secret value");
        assert_eq!(secret.code(), NodeGraphErrorCode::InvalidConfig);

        let oversized = NodeConfig::new(
            id(1),
            "stillflow.node.output",
            1,
            json!({"outputLabel": "x".repeat(MAX_STRING_BYTES + 1)}),
            BTreeMap::new(),
        )
        .expect_err("string bound");
        assert_eq!(oversized.code(), NodeGraphErrorCode::LimitStringBytes);

        let too_large = vec![b' '; MAX_GRAPH_BYTES + 1];
        let error = NodeGraph::from_json_bytes(&too_large, &NodeRegistry::new())
            .expect_err("graph byte bound");
        assert_eq!(error.code(), NodeGraphErrorCode::LimitGraphBytes);

        let mut metadata = BTreeMap::new();
        metadata.insert("apiKey".to_owned(), "hidden".to_owned());
        let error = NodeConfig::new(
            id(1),
            "stillflow.node.output",
            1,
            json!({"outputLabel": "clean"}),
            metadata,
        )
        .expect_err("secret metadata");
        assert_eq!(error.code(), NodeGraphErrorCode::InvalidConfig);
    }

    #[test]
    fn typed_builtin_configs_validate_without_execution_types() {
        let registry = NodeRegistry::new();
        let graph = graph();
        let configs = graph.validated_configs(&registry).expect("configs");
        assert!(matches!(
            configs.get(&id(1)),
            Some(ValidatedNodeConfig::Source { .. })
        ));
        assert!(matches!(
            configs.get(&id(2)),
            Some(ValidatedNodeConfig::Select { .. })
        ));
        assert!(matches!(
            configs.get(&id(3)),
            Some(ValidatedNodeConfig::Output { output_label })
                if output_label == "clean"
        ));
    }
}
