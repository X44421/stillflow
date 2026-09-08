use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;
use uuid::Uuid;

use crate::{ensure_no_secret_fields, ColumnId, Expr, LogicalType, ScalarValue};

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
    message: String,
}

impl NodeGraphError {
    fn new(code: NodeGraphErrorCode, node_id: Option<NodeId>, message: impl Into<String>) -> Self {
        Self {
            code,
            node_id,
            message: message.into(),
        }
    }

    pub const fn code(&self) -> NodeGraphErrorCode {
        self.code
    }

    pub const fn node_id(&self) -> Option<NodeId> {
        self.node_id
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
#[serde(rename_all = "camelCase")]
pub struct NodePort {
    pub node_id: NodeId,
    pub port: PortId,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
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
#[serde(rename_all = "camelCase")]
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
#[serde(rename_all = "camelCase")]
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
        for node in &self.nodes {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ConfigValueKind {
    Uuid,
    ColumnId,
    ColumnIdList,
    String,
    Boolean,
    Expression,
    LogicalType,
    ScalarValue,
    CastFailurePolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigField {
    pub name: String,
    pub value_kind: ConfigValueKind,
    pub required: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigSchema {
    pub fields: Vec<ConfigField>,
    pub additional_properties: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum NodeLoweringTarget {
    Scan,
    Project,
    Filter,
    ApplyRules,
    Materialize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum NodeSupportStatus {
    Supported,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeCatalogEntry {
    pub type_id: String,
    pub config_version: u16,
    pub display_name: String,
    pub description: String,
    pub config_schema: ConfigSchema,
    pub input_ports: Vec<PortId>,
    pub output_ports: Vec<PortId>,
    pub lowering_target: NodeLoweringTarget,
    pub support_status: NodeSupportStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NodeDefinitionKind {
    Source,
    Select,
    Filter,
    Rename,
    Trim,
    Cast,
    ReplaceLiteral,
    FillNull,
    DropColumn,
    DeriveColumn,
    Output,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NodeRole {
    Source,
    Transform,
    Output,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeDefinition {
    type_id: String,
    config_version: u16,
    display_name: String,
    description: String,
    config_schema: ConfigSchema,
    input_ports: Vec<PortId>,
    output_ports: Vec<PortId>,
    lowering_target: NodeLoweringTarget,
    support_status: NodeSupportStatus,
    kind: NodeDefinitionKind,
}

impl NodeDefinition {
    pub fn type_id(&self) -> &str {
        &self.type_id
    }

    pub const fn config_version(&self) -> u16 {
        self.config_version
    }

    pub fn display_name(&self) -> &str {
        &self.display_name
    }

    pub fn description(&self) -> &str {
        &self.description
    }

    pub fn config_schema(&self) -> &ConfigSchema {
        &self.config_schema
    }

    pub fn input_ports(&self) -> &[PortId] {
        &self.input_ports
    }

    pub fn output_ports(&self) -> &[PortId] {
        &self.output_ports
    }

    pub const fn lowering_target(&self) -> NodeLoweringTarget {
        self.lowering_target
    }

    pub const fn support_status(&self) -> NodeSupportStatus {
        self.support_status
    }

    pub fn catalog_entry(&self) -> NodeCatalogEntry {
        NodeCatalogEntry {
            type_id: self.type_id.clone(),
            config_version: self.config_version,
            display_name: self.display_name.clone(),
            description: self.description.clone(),
            config_schema: self.config_schema.clone(),
            input_ports: self.input_ports.clone(),
            output_ports: self.output_ports.clone(),
            lowering_target: self.lowering_target,
            support_status: self.support_status,
        }
    }

    fn role(&self) -> NodeRole {
        match self.kind {
            NodeDefinitionKind::Source => NodeRole::Source,
            NodeDefinitionKind::Output => NodeRole::Output,
            _ => NodeRole::Transform,
        }
    }

    fn is_source(&self) -> bool {
        self.role() == NodeRole::Source
    }

    fn is_output(&self) -> bool {
        self.role() == NodeRole::Output
    }

    fn validate_node_config(
        &self,
        node: &NodeConfig,
    ) -> Result<ValidatedNodeConfig, NodeGraphError> {
        if node.type_id() != self.type_id {
            return Err(NodeGraphError::new(
                NodeGraphErrorCode::UnknownNodeType,
                Some(node.id()),
                "node type does not match its resolved definition",
            ));
        }
        if node.config_version() != self.config_version {
            return Err(NodeGraphError::new(
                NodeGraphErrorCode::UnsupportedConfigVersion,
                Some(node.id()),
                format!(
                    "config version {} is not supported for {}",
                    node.config_version, self.type_id
                ),
            ));
        }

        match self.kind {
            NodeDefinitionKind::Source => {
                let data: SourceConfigData =
                    parse_config(node, &[("sourceAssetId", true)], &["projection"])?;
                if data.source_asset_id.is_nil() {
                    return Err(invalid_config(node, "sourceAssetId"));
                }
                if node.config.get("projection").is_some_and(Value::is_null) {
                    return Err(invalid_config(node, "projection"));
                }
                if let Some(projection) = &data.projection {
                    if projection.is_empty() {
                        return Err(invalid_config(node, "projection"));
                    }
                    validate_columns(node, projection, "projection")?;
                }
                Ok(ValidatedNodeConfig::Source {
                    source_asset_id: data.source_asset_id,
                    projection: data.projection,
                })
            }
            NodeDefinitionKind::Select => {
                let data: SelectConfigData = parse_config(node, &[("columns", true)], &[])?;
                if data.columns.is_empty() {
                    return Err(invalid_config(node, "columns"));
                }
                validate_columns(node, &data.columns, "columns")?;
                Ok(ValidatedNodeConfig::Select {
                    columns: data.columns,
                })
            }
            NodeDefinitionKind::Filter => {
                let data: FilterConfigData = parse_config(node, &[("predicate", true)], &[])?;
                validate_expression(node, &data.predicate, "predicate")?;
                Ok(ValidatedNodeConfig::Filter {
                    predicate: data.predicate,
                })
            }
            NodeDefinitionKind::Rename => {
                let data: RenameConfigData =
                    parse_config(node, &[("column", true), ("to", true)], &[])?;
                validate_column(node, data.column, "column")?;
                validate_name(node, &data.to, "to")?;
                Ok(ValidatedNodeConfig::Rename {
                    column: data.column,
                    to: data.to,
                })
            }
            NodeDefinitionKind::Trim => {
                let data: ColumnConfigData = parse_config(node, &[("column", true)], &[])?;
                validate_column(node, data.column, "column")?;
                Ok(ValidatedNodeConfig::Trim {
                    column: data.column,
                })
            }
            NodeDefinitionKind::Cast => {
                let data: CastConfigData = parse_config(
                    node,
                    &[("column", true), ("dataType", true), ("onFailure", true)],
                    &[],
                )?;
                validate_column(node, data.column, "column")?;
                validate_logical_type(node, &data.data_type, "dataType")?;
                Ok(ValidatedNodeConfig::Cast {
                    column: data.column,
                    data_type: data.data_type,
                    on_failure: data.on_failure,
                })
            }
            NodeDefinitionKind::ReplaceLiteral => {
                let data: ReplaceLiteralConfigData =
                    parse_config(node, &[("column", true), ("from", true), ("to", true)], &[])?;
                validate_column(node, data.column, "column")?;
                validate_scalar(node, &data.from, "from")?;
                validate_scalar(node, &data.to, "to")?;
                Ok(ValidatedNodeConfig::ReplaceLiteral {
                    column: data.column,
                    from: data.from,
                    to: data.to,
                })
            }
            NodeDefinitionKind::FillNull => {
                let data: FillNullConfigData =
                    parse_config(node, &[("column", true), ("value", true)], &[])?;
                validate_column(node, data.column, "column")?;
                if matches!(data.value, ScalarValue::Null) {
                    return Err(invalid_config(node, "value"));
                }
                validate_scalar(node, &data.value, "value")?;
                Ok(ValidatedNodeConfig::FillNull {
                    column: data.column,
                    value: data.value,
                })
            }
            NodeDefinitionKind::DropColumn => {
                let data: ColumnConfigData = parse_config(node, &[("column", true)], &[])?;
                validate_column(node, data.column, "column")?;
                Ok(ValidatedNodeConfig::DropColumn {
                    column: data.column,
                })
            }
            NodeDefinitionKind::DeriveColumn => {
                let data: DeriveColumnConfigData = parse_config(
                    node,
                    &[
                        ("id", true),
                        ("name", true),
                        ("dataType", true),
                        ("nullable", true),
                        ("expression", true),
                    ],
                    &[],
                )?;
                validate_column(node, data.id, "id")?;
                validate_name(node, &data.name, "name")?;
                validate_logical_type(node, &data.data_type, "dataType")?;
                validate_expression(node, &data.expression, "expression")?;
                Ok(ValidatedNodeConfig::DeriveColumn {
                    id: data.id,
                    name: data.name,
                    data_type: data.data_type,
                    nullable: data.nullable,
                    expression: data.expression,
                })
            }
            NodeDefinitionKind::Output => {
                let data: OutputConfigData = parse_config(node, &[("outputLabel", true)], &[])?;
                validate_name(node, &data.output_label, "outputLabel")?;
                Ok(ValidatedNodeConfig::Output {
                    output_label: data.output_label,
                })
            }
        }
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
    pub fn new() -> Self {
        let mut definitions = vec![
            source_definition(),
            select_definition(),
            filter_definition(),
            rename_definition(),
            trim_definition(),
            cast_definition(),
            replace_literal_definition(),
            fill_null_definition(),
            drop_column_definition(),
            derive_column_definition(),
            output_definition(),
        ];
        definitions.sort_by(|left, right| {
            left.type_id
                .cmp(&right.type_id)
                .then(left.config_version.cmp(&right.config_version))
        });
        debug_assert!(definitions
            .windows(2)
            .all(|pair| pair[0].type_id != pair[1].type_id
                || pair[0].config_version != pair[1].config_version));
        Self { definitions }
    }

    pub fn definitions(&self) -> &[NodeDefinition] {
        &self.definitions
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
            .any(|definition| definition.type_id == type_id);
        self.definitions
            .binary_search_by(|definition| {
                definition
                    .type_id
                    .as_str()
                    .cmp(type_id)
                    .then(definition.config_version.cmp(&config_version))
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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SourceConfigData {
    source_asset_id: Uuid,
    #[serde(default)]
    projection: Option<Vec<ColumnId>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SelectConfigData {
    columns: Vec<ColumnId>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FilterConfigData {
    predicate: Expr,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RenameConfigData {
    column: ColumnId,
    to: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ColumnConfigData {
    column: ColumnId,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CastConfigData {
    column: ColumnId,
    data_type: LogicalType,
    on_failure: CastFailurePolicy,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ReplaceLiteralConfigData {
    column: ColumnId,
    from: ScalarValue,
    to: ScalarValue,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FillNullConfigData {
    column: ColumnId,
    value: ScalarValue,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DeriveColumnConfigData {
    id: ColumnId,
    name: String,
    data_type: LogicalType,
    nullable: bool,
    expression: Expr,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct OutputConfigData {
    output_label: String,
}

fn source_definition() -> NodeDefinition {
    definition(
        "stillflow.node.source",
        "Source",
        "Authorized source asset input.",
        NodeDefinitionKind::Source,
        NodeLoweringTarget::Scan,
        vec![
            config_field("sourceAssetId", ConfigValueKind::Uuid, true),
            config_field("projection", ConfigValueKind::ColumnIdList, false),
        ],
        Vec::new(),
        vec![PortId::from_static("out")],
    )
}

fn select_definition() -> NodeDefinition {
    definition(
        "stillflow.node.select",
        "Select",
        "Keep an ordered set of existing columns.",
        NodeDefinitionKind::Select,
        NodeLoweringTarget::Project,
        vec![config_field("columns", ConfigValueKind::ColumnIdList, true)],
        vec![PortId::from_static("in")],
        vec![PortId::from_static("out")],
    )
}

fn filter_definition() -> NodeDefinition {
    definition(
        "stillflow.node.filter",
        "Filter",
        "Filter rows with a logical predicate.",
        NodeDefinitionKind::Filter,
        NodeLoweringTarget::Filter,
        vec![config_field("predicate", ConfigValueKind::Expression, true)],
        vec![PortId::from_static("in")],
        vec![PortId::from_static("out")],
    )
}

fn rename_definition() -> NodeDefinition {
    definition(
        "stillflow.node.rename",
        "Rename",
        "Rename one logical column without changing its identity.",
        NodeDefinitionKind::Rename,
        NodeLoweringTarget::ApplyRules,
        vec![
            config_field("column", ConfigValueKind::ColumnId, true),
            config_field("to", ConfigValueKind::String, true),
        ],
        vec![PortId::from_static("in")],
        vec![PortId::from_static("out")],
    )
}

fn trim_definition() -> NodeDefinition {
    definition(
        "stillflow.node.trim",
        "Trim",
        "Trim whitespace from one UTF-8 column.",
        NodeDefinitionKind::Trim,
        NodeLoweringTarget::ApplyRules,
        vec![config_field("column", ConfigValueKind::ColumnId, true)],
        vec![PortId::from_static("in")],
        vec![PortId::from_static("out")],
    )
}

fn cast_definition() -> NodeDefinition {
    definition(
        "stillflow.node.cast",
        "Cast",
        "Cast one logical column with an explicit failure policy.",
        NodeDefinitionKind::Cast,
        NodeLoweringTarget::ApplyRules,
        vec![
            config_field("column", ConfigValueKind::ColumnId, true),
            config_field("dataType", ConfigValueKind::LogicalType, true),
            config_field("onFailure", ConfigValueKind::CastFailurePolicy, true),
        ],
        vec![PortId::from_static("in")],
        vec![PortId::from_static("out")],
    )
}

fn replace_literal_definition() -> NodeDefinition {
    definition(
        "stillflow.node.replace-literal",
        "Replace literal",
        "Replace one typed literal in a column.",
        NodeDefinitionKind::ReplaceLiteral,
        NodeLoweringTarget::ApplyRules,
        vec![
            config_field("column", ConfigValueKind::ColumnId, true),
            config_field("from", ConfigValueKind::ScalarValue, true),
            config_field("to", ConfigValueKind::ScalarValue, true),
        ],
        vec![PortId::from_static("in")],
        vec![PortId::from_static("out")],
    )
}

fn fill_null_definition() -> NodeDefinition {
    definition(
        "stillflow.node.fill-null",
        "Fill null",
        "Fill null values with one non-null literal.",
        NodeDefinitionKind::FillNull,
        NodeLoweringTarget::ApplyRules,
        vec![
            config_field("column", ConfigValueKind::ColumnId, true),
            config_field("value", ConfigValueKind::ScalarValue, true),
        ],
        vec![PortId::from_static("in")],
        vec![PortId::from_static("out")],
    )
}

fn drop_column_definition() -> NodeDefinition {
    definition(
        "stillflow.node.drop-column",
        "Drop column",
        "Remove one logical column.",
        NodeDefinitionKind::DropColumn,
        NodeLoweringTarget::ApplyRules,
        vec![config_field("column", ConfigValueKind::ColumnId, true)],
        vec![PortId::from_static("in")],
        vec![PortId::from_static("out")],
    )
}

fn derive_column_definition() -> NodeDefinition {
    definition(
        "stillflow.node.derive-column",
        "Derive column",
        "Append one caller-identified column from a logical expression.",
        NodeDefinitionKind::DeriveColumn,
        NodeLoweringTarget::ApplyRules,
        vec![
            config_field("id", ConfigValueKind::ColumnId, true),
            config_field("name", ConfigValueKind::String, true),
            config_field("dataType", ConfigValueKind::LogicalType, true),
            config_field("nullable", ConfigValueKind::Boolean, true),
            config_field("expression", ConfigValueKind::Expression, true),
        ],
        vec![PortId::from_static("in")],
        vec![PortId::from_static("out")],
    )
}

fn output_definition() -> NodeDefinition {
    definition(
        "stillflow.node.output",
        "Output",
        "Declare the logical materialization label.",
        NodeDefinitionKind::Output,
        NodeLoweringTarget::Materialize,
        vec![config_field("outputLabel", ConfigValueKind::String, true)],
        vec![PortId::from_static("in")],
        Vec::new(),
    )
}

#[allow(clippy::too_many_arguments)]
fn definition(
    type_id: &str,
    display_name: &str,
    description: &str,
    kind: NodeDefinitionKind,
    lowering_target: NodeLoweringTarget,
    fields: Vec<ConfigField>,
    input_ports: Vec<PortId>,
    output_ports: Vec<PortId>,
) -> NodeDefinition {
    NodeDefinition {
        type_id: type_id.to_owned(),
        config_version: 1,
        display_name: display_name.to_owned(),
        description: description.to_owned(),
        config_schema: ConfigSchema {
            fields,
            additional_properties: false,
        },
        input_ports,
        output_ports,
        lowering_target,
        support_status: NodeSupportStatus::Supported,
        kind,
    }
}

fn config_field(name: &str, value_kind: ConfigValueKind, required: bool) -> ConfigField {
    ConfigField {
        name: name.to_owned(),
        value_kind,
        required,
    }
}

fn parse_config<T: DeserializeOwned>(
    node: &NodeConfig,
    required: &[(&str, bool)],
    optional: &[&str],
) -> Result<T, NodeGraphError> {
    let object = node.config.as_object().ok_or_else(|| {
        NodeGraphError::new(
            NodeGraphErrorCode::InvalidConfig,
            Some(node.id()),
            "node config must be an object",
        )
    })?;
    let allowed: BTreeSet<&str> = required
        .iter()
        .map(|(name, _)| *name)
        .chain(optional.iter().copied())
        .collect();
    for key in object.keys() {
        if !allowed.contains(key.as_str()) {
            return Err(invalid_config(node, key));
        }
    }
    for (name, required_field) in required {
        if *required_field && !object.contains_key(*name) {
            return Err(invalid_config(node, name));
        }
    }
    serde_json::from_value(node.config.clone()).map_err(|_| {
        NodeGraphError::new(
            NodeGraphErrorCode::InvalidConfig,
            Some(node.id()),
            "node config has an invalid field shape or primitive type",
        )
    })
}

fn invalid_config(node: &NodeConfig, field: &str) -> NodeGraphError {
    NodeGraphError::new(
        NodeGraphErrorCode::InvalidConfig,
        Some(node.id()),
        format!("node config field {field} is missing or invalid"),
    )
}

fn validate_name(node: &NodeConfig, value: &str, field: &str) -> Result<(), NodeGraphError> {
    validate_string(value, Some(node.id()), field)?;
    if value.trim().is_empty() {
        return Err(invalid_config(node, field));
    }
    Ok(())
}

fn validate_column(node: &NodeConfig, column: ColumnId, field: &str) -> Result<(), NodeGraphError> {
    if column.as_uuid().is_nil() {
        return Err(invalid_config(node, field));
    }
    Ok(())
}

fn validate_columns(
    node: &NodeConfig,
    columns: &[ColumnId],
    field: &str,
) -> Result<(), NodeGraphError> {
    let mut unique = BTreeSet::new();
    for column in columns {
        validate_column(node, *column, field)?;
        if !unique.insert(*column) {
            return Err(invalid_config(node, field));
        }
    }
    Ok(())
}

fn validate_expression(
    node: &NodeConfig,
    expression: &Expr,
    field: &str,
) -> Result<(), NodeGraphError> {
    expression
        .validate_shape()
        .map_err(|_| invalid_config(node, field))?;
    let mut pending = vec![(expression, 1_usize)];
    let mut count = 0_usize;
    while let Some((current, depth)) = pending.pop() {
        count = count.checked_add(1).ok_or_else(|| {
            NodeGraphError::new(
                NodeGraphErrorCode::LimitCompileWork,
                Some(node.id()),
                "expression node count overflowed",
            )
        })?;
        if count > MAX_EXPR_NODES {
            return Err(NodeGraphError::new(
                NodeGraphErrorCode::LimitCompileWork,
                Some(node.id()),
                "expression node bound exceeded",
            ));
        }
        if depth > MAX_EXPR_DEPTH {
            return Err(NodeGraphError::new(
                NodeGraphErrorCode::LimitNestingDepth,
                Some(node.id()),
                "expression depth bound exceeded",
            ));
        }
        match current {
            Expr::Column(column) => {
                if column.as_uuid().is_nil() {
                    return Err(invalid_config(node, field));
                }
            }
            Expr::Literal(_) => {}
            Expr::Unary { expression, .. }
            | Expr::IsNull { expression, .. }
            | Expr::Cast { expression, .. } => {
                pending.push((expression, depth + 1));
            }
            Expr::Binary { left, right, .. } => {
                pending.push((left, depth + 1));
                pending.push((right, depth + 1));
            }
            Expr::Coalesce { expressions } => {
                for expression in expressions {
                    pending.push((expression, depth + 1));
                }
            }
        }
    }
    Ok(())
}

fn validate_logical_type(
    node: &NodeConfig,
    data_type: &LogicalType,
    field: &str,
) -> Result<(), NodeGraphError> {
    data_type
        .validate()
        .map_err(|_| invalid_config(node, field))
}

fn validate_scalar(
    node: &NodeConfig,
    value: &ScalarValue,
    field: &str,
) -> Result<(), NodeGraphError> {
    Expr::Literal(value.clone())
        .validate_shape()
        .map_err(|_| invalid_config(node, field))
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
