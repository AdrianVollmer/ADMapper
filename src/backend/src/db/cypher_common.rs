//! Shared implementation for Cypher-based database backends (FalkorDB, Neo4j).
//!
//! Both FalkorDB and Neo4j use Cypher queries and share the vast majority of
//! their `DatabaseBackend` logic. This module extracts the common code so
//! changes only need to be made in one place.
//!
//! Each backend implements `CypherExecutor` (a thin wrapper around their
//! native query execution) and then delegates shared trait methods to the
//! free functions in this module.

use serde_json::{Map, Value as JsonValue};
use std::collections::HashSet;
use tracing::debug;

use super::types::{
    json_to_cypher_props, CypherEscapeStyle, DbEdge, DbNode, DetailedStats, ReachabilityInsight,
    Result, ADMIN_RELATIONSHIP_TYPES, WELL_KNOWN_PRINCIPALS,
};

// ========================================================================
// Constants
// ========================================================================

/// Batch size for node and edge import operations.
pub const BATCH_SIZE: usize = 500;

/// BloodHound node labels that need objectid indexes.
pub const NODE_LABELS: &[&str] = &[
    "User",
    "Computer",
    "Group",
    "Domain",
    "OU",
    "GPO",
    "Container",
    "CertTemplate",
    "EnterpriseCA",
    "RootCA",
    "AIACA",
    "NTAuthStore",
    "Base",
];

// ========================================================================
// Helpers
// ========================================================================

/// Build a Cypher list literal for admin relationship types, e.g.
/// `'AdminTo', 'GenericAll', ...`
pub fn admin_types_cypher_list() -> String {
    ADMIN_RELATIONSHIP_TYPES
        .iter()
        .map(|t| format!("'{}'", t))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Build a WHERE clause fragment to exclude certain edge types from a path.
/// Returns an empty string if no types are excluded.
pub fn build_exclude_clause(exclude_relationship_types: &[String]) -> String {
    if exclude_relationship_types.is_empty() {
        String::new()
    } else {
        let types: Vec<String> = exclude_relationship_types
            .iter()
            .map(|t| format!("'{}'", t))
            .collect();
        format!(
            "AND NONE(r IN relationships(p) WHERE type(r) IN [{}])",
            types.join(", ")
        )
    }
}

/// Parse a node from the JSON representation produced by both backends.
///
/// Expected shape: `{ "labels": [...], "properties": { "objectid": ..., "name": ... } }`
pub fn parse_node_from_value(value: &JsonValue) -> Option<DbNode> {
    let obj = value.as_object()?;

    let id = obj
        .get("properties")
        .and_then(|p| p.get("objectid"))
        .and_then(|v| v.as_str())
        .or_else(|| obj.get("objectid").and_then(|v| v.as_str()))
        .map(|s| s.to_string())
        .unwrap_or_default();

    let name = obj
        .get("properties")
        .and_then(|p| p.get("name"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| id.clone());

    let label = obj
        .get("labels")
        .and_then(|l| l.as_array())
        .and_then(|arr| {
            arr.iter()
                .find(|v| v.as_str() != Some("Base"))
                .or_else(|| arr.first())
        })
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| "Unknown".to_string());

    let properties = obj
        .get("properties")
        .cloned()
        .unwrap_or(JsonValue::Object(Map::new()));

    Some(DbNode {
        id,
        name,
        label,
        properties,
    })
}

/// Count total users in the database.
pub fn count_total_users(exec: &impl CypherExecutor) -> Result<usize> {
    let rows = exec.exec_rows("MATCH (n:User) RETURN count(n) AS count")?;
    Ok(rows
        .first()
        .and_then(|r| r.first())
        .and_then(|v| v.as_i64())
        .unwrap_or(0) as usize)
}

/// Compute reachability from well-known principals.
///
/// Uses direct-neighbor count (excluding MemberOf) to avoid expensive
/// variable-length path traversals.
pub fn compute_reachability(exec: &impl CypherExecutor) -> Vec<ReachabilityInsight> {
    let mut reachability = Vec::new();
    for (name, pattern) in WELL_KNOWN_PRINCIPALS {
        let cypher = if pattern.starts_with('-') {
            format!(
                "MATCH (p) WHERE p.objectid ENDS WITH '{}' \
                 OPTIONAL MATCH (p)-[r]->(t) WHERE type(r) <> 'MemberOf' \
                 RETURN p.objectid AS id, count(DISTINCT t) AS cnt LIMIT 1",
                pattern
            )
        } else {
            format!(
                "MATCH (p {{objectid: '{}'}}) \
                 OPTIONAL MATCH (p)-[r]->(t) WHERE type(r) <> 'MemberOf' \
                 RETURN p.objectid AS id, count(DISTINCT t) AS cnt LIMIT 1",
                pattern
            )
        };

        let rows = exec.exec_rows(&cypher).unwrap_or_default();
        let (principal_id, reachable_count) = rows
            .first()
            .map(|r| {
                let id = r.first().and_then(|v| v.as_str()).map(|s| s.to_string());
                let cnt = r.get(1).and_then(|v| v.as_i64()).unwrap_or(0) as usize;
                (id, cnt)
            })
            .unwrap_or((None, 0));

        reachability.push(ReachabilityInsight {
            principal_name: name.to_string(),
            principal_id,
            reachable_count,
        });
    }
    reachability
}

/// Parse real DA rows (id, name) from positional query results.
pub fn parse_real_das(rows: &[Vec<JsonValue>]) -> Vec<(String, String)> {
    rows.iter()
        .filter_map(|r| {
            let id = r.first()?.as_str()?.to_string();
            let name = r.get(1).and_then(|v| v.as_str()).unwrap_or(&id).to_string();
            Some((id, name))
        })
        .collect()
}

/// Parse paths-to-DA results (id, label, name, hops) with deduplication.
pub fn parse_paths_to_da_results(rows: &[Vec<JsonValue>]) -> Vec<(String, String, String, usize)> {
    let mut results = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();

    for row in rows {
        if row.len() >= 3 {
            if let (Some(id), Some(name), Some(hops)) =
                (row[0].as_str(), row[1].as_str(), row[2].as_i64())
            {
                if !seen.contains(id) {
                    seen.insert(id.to_string());
                    results.push((
                        id.to_string(),
                        "User".to_string(),
                        name.to_string(),
                        hops as usize,
                    ));
                }
            }
        }
    }

    results
}

// ========================================================================
// CypherExecutor trait
// ========================================================================

/// Trait for executing Cypher queries, implemented by both FalkorDB and Neo4j.
///
/// Results are normalized to positional JSON arrays so that shared logic
/// can parse them identically regardless of the underlying driver.
pub(crate) trait CypherExecutor {
    /// Execute a read query, returning rows as positional JSON value arrays.
    fn exec_rows(&self, cypher: &str) -> Result<Vec<Vec<JsonValue>>>;

    /// Execute a write-only query.
    fn exec_write(&self, cypher: &str) -> Result<()>;
}

// ========================================================================
// Shared DatabaseBackend implementations
// ========================================================================

pub fn get_stats(exec: &impl CypherExecutor) -> Result<(usize, usize)> {
    let node_rows = exec.exec_rows("MATCH (n) RETURN count(n) AS count")?;
    let node_count = node_rows
        .first()
        .and_then(|r| r.first())
        .and_then(|v| v.as_i64())
        .unwrap_or(0) as usize;

    let edge_rows = exec.exec_rows("MATCH ()-[r]->() RETURN count(r) AS count")?;
    let edge_count = edge_rows
        .first()
        .and_then(|r| r.first())
        .and_then(|v| v.as_i64())
        .unwrap_or(0) as usize;

    Ok((node_count, edge_count))
}

pub fn get_detailed_stats(exec: &impl CypherExecutor) -> Result<DetailedStats> {
    let (total_nodes, total_edges) = get_stats(exec)?;

    let rows = exec.exec_rows(
        "MATCH (n) WITH [l IN labels(n) WHERE l <> 'Base'][0] AS label \
         RETURN label, count(*) AS count",
    )?;

    let mut type_counts: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();
    for row in rows {
        if row.len() >= 2 {
            if let (Some(label), Some(count)) = (row[0].as_str(), row[1].as_i64()) {
                type_counts.insert(label.to_string(), count as usize);
            }
        }
    }

    Ok(DetailedStats {
        total_nodes,
        total_edges,
        users: type_counts.get("User").copied().unwrap_or(0),
        computers: type_counts.get("Computer").copied().unwrap_or(0),
        groups: type_counts.get("Group").copied().unwrap_or(0),
        domains: type_counts.get("Domain").copied().unwrap_or(0),
        ous: type_counts.get("OU").copied().unwrap_or(0),
        gpos: type_counts.get("GPO").copied().unwrap_or(0),
        database_size_bytes: None,
        cache_entries: None,
        cache_size_bytes: None,
    })
}

pub fn get_all_nodes(exec: &impl CypherExecutor) -> Result<Vec<DbNode>> {
    let rows = exec.exec_rows("MATCH (n) RETURN n")?;
    let nodes = rows
        .iter()
        .filter_map(|r| r.first())
        .filter_map(parse_node_from_value)
        .collect();
    Ok(nodes)
}

pub fn get_all_edges(exec: &impl CypherExecutor) -> Result<Vec<DbEdge>> {
    let rows = exec.exec_rows(
        "MATCH (a)-[r]->(b) \
         RETURN a.objectid AS src, b.objectid AS tgt, type(r) AS typ, r AS rel",
    )?;

    let relationships = rows
        .iter()
        .filter_map(|r| {
            let src = r.first()?.as_str()?.to_string();
            let tgt = r.get(1)?.as_str()?.to_string();
            let typ = r.get(2)?.as_str()?.to_string();
            let props = r
                .get(3)
                .and_then(|v| v.get("properties"))
                .cloned()
                .unwrap_or(JsonValue::Object(Map::new()));
            Some(DbEdge {
                source: src,
                target: tgt,
                rel_type: typ,
                properties: props,
                ..Default::default()
            })
        })
        .collect();

    Ok(relationships)
}

pub fn get_nodes_by_ids(exec: &impl CypherExecutor, ids: &[String]) -> Result<Vec<DbNode>> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }

    let id_list: Vec<String> = ids
        .iter()
        .map(|id| format!("'{}'", id.replace('\'', "\\'")))
        .collect();

    let cypher = format!(
        "MATCH (n) WHERE n.objectid IN [{}] RETURN n",
        id_list.join(", ")
    );

    let rows = exec.exec_rows(&cypher)?;
    let nodes = rows
        .iter()
        .filter_map(|r| r.first())
        .filter_map(parse_node_from_value)
        .collect();

    Ok(nodes)
}

pub fn get_edges_between(exec: &impl CypherExecutor, node_ids: &[String]) -> Result<Vec<DbEdge>> {
    if node_ids.is_empty() {
        return Ok(Vec::new());
    }

    let id_list: Vec<String> = node_ids
        .iter()
        .map(|id| format!("'{}'", id.replace('\'', "\\'")))
        .collect();
    let id_set = id_list.join(", ");

    let cypher = format!(
        "MATCH (a)-[r]->(b) \
         WHERE a.objectid IN [{id_set}] AND b.objectid IN [{id_set}] \
         RETURN a.objectid AS src, b.objectid AS tgt, type(r) AS typ, r AS rel"
    );

    let rows = exec.exec_rows(&cypher)?;
    let relationships = rows
        .iter()
        .filter_map(|r| {
            let src = r.first()?.as_str()?.to_string();
            let tgt = r.get(1)?.as_str()?.to_string();
            let typ = r.get(2)?.as_str()?.to_string();
            let props = r
                .get(3)
                .and_then(|v| v.get("properties"))
                .cloned()
                .unwrap_or(JsonValue::Object(Map::new()));
            Some(DbEdge {
                source: src,
                target: tgt,
                rel_type: typ,
                properties: props,
                ..Default::default()
            })
        })
        .collect();

    Ok(relationships)
}

pub fn get_relationship_types(exec: &impl CypherExecutor) -> Result<Vec<String>> {
    let rows = exec.exec_rows("MATCH ()-[r]->() RETURN DISTINCT type(r) AS typ")?;
    let types = rows
        .iter()
        .filter_map(|r| r.first())
        .filter_map(|v| v.as_str())
        .map(|s| s.to_string())
        .collect();
    Ok(types)
}

pub fn get_node_types(exec: &impl CypherExecutor) -> Result<Vec<String>> {
    let rows = exec.exec_rows(
        "MATCH (n) UNWIND labels(n) AS label \
         WITH DISTINCT label WHERE label <> 'Base' \
         RETURN label ORDER BY label",
    )?;
    let types = rows
        .iter()
        .filter_map(|r| r.first())
        .filter_map(|v| v.as_str())
        .map(|s| s.to_string())
        .collect();
    Ok(types)
}

pub fn search_nodes(
    exec: &impl CypherExecutor,
    search_query: &str,
    limit: usize,
    label: Option<&str>,
) -> Result<Vec<DbNode>> {
    let pattern = search_query.replace('\'', "\\'").to_lowercase();

    let match_clause = match label {
        Some(l) => format!("MATCH (n:{l})"),
        None => "MATCH (n)".to_string(),
    };

    let cypher = format!(
        "{match_clause} WHERE toLower(n.name) CONTAINS '{pattern}' \
         OR toLower(n.objectid) CONTAINS '{pattern}' \
         RETURN n LIMIT {limit}"
    );

    let rows = exec.exec_rows(&cypher)?;
    let mut nodes: Vec<DbNode> = rows
        .iter()
        .filter_map(|r| r.first())
        .filter_map(parse_node_from_value)
        .collect();

    nodes.sort_by(|a, b| {
        a.name
            .to_lowercase()
            .cmp(&b.name.to_lowercase())
            .then_with(|| a.id.cmp(&b.id))
    });

    debug!(query = %search_query, found = nodes.len(), "Search complete");
    Ok(nodes)
}

pub fn resolve_node_identifier(
    exec: &impl CypherExecutor,
    identifier: &str,
) -> Result<Option<String>> {
    let id_escaped = identifier.replace('\'', "\\'");

    // Try exact objectid match
    let cypher = format!("MATCH (n {{objectid: '{id_escaped}'}}) RETURN n.objectid AS id LIMIT 1");
    let rows = exec.exec_rows(&cypher)?;
    if let Some(id) = rows
        .first()
        .and_then(|r| r.first())
        .and_then(|v| v.as_str())
    {
        return Ok(Some(id.to_string()));
    }

    // Try case-insensitive name match
    let cypher = format!(
        "MATCH (n) WHERE toLower(n.name) = toLower('{id_escaped}') \
         RETURN n.objectid AS id LIMIT 1"
    );
    let rows = exec.exec_rows(&cypher)?;
    if let Some(id) = rows
        .first()
        .and_then(|r| r.first())
        .and_then(|v| v.as_str())
    {
        return Ok(Some(id.to_string()));
    }

    Ok(None)
}

pub fn get_node_connections(
    exec: &impl CypherExecutor,
    node_id: &str,
    direction: &str,
) -> Result<(Vec<DbNode>, Vec<DbEdge>)> {
    debug!(node_id = %node_id, direction = %direction, "Getting node connections");

    let id_escaped = node_id.replace('\'', "\\'");
    let admin_types = admin_types_cypher_list();

    let cypher = match direction {
        "incoming" => format!("MATCH (a)-[r]->(b {{objectid: '{id_escaped}'}}) RETURN a, r, b"),
        "outgoing" => format!("MATCH (a {{objectid: '{id_escaped}'}})-[r]->(b) RETURN a, r, b"),
        "admin" => format!(
            "MATCH (a {{objectid: '{id_escaped}'}})-[r]->(b) \
             WHERE type(r) IN [{admin_types}] \
             RETURN a, r, b"
        ),
        "memberof" => {
            format!("MATCH (a {{objectid: '{id_escaped}'}})-[r:MemberOf]->(b) RETURN a, r, b")
        }
        "members" => {
            format!("MATCH (a)-[r:MemberOf]->(b {{objectid: '{id_escaped}'}}) RETURN a, r, b")
        }
        _ => format!("MATCH (a {{objectid: '{id_escaped}'}})-[r]-(b) RETURN a, r, b"),
    };

    let rows = exec.exec_rows(&cypher)?;

    let mut node_ids: HashSet<String> = HashSet::new();
    node_ids.insert(node_id.to_string());

    let mut relationships = Vec::new();
    for row in &rows {
        if row.len() >= 3 {
            if let (Some(src_node), Some(tgt_node)) = (
                parse_node_from_value(&row[0]),
                parse_node_from_value(&row[2]),
            ) {
                node_ids.insert(src_node.id.clone());
                node_ids.insert(tgt_node.id.clone());

                if let Some(rel) = row[1].as_object() {
                    let rel_type = rel
                        .get("rel_type")
                        .and_then(|v| v.as_str())
                        .unwrap_or("RELATED")
                        .to_string();
                    let props = rel
                        .get("properties")
                        .cloned()
                        .unwrap_or(JsonValue::Object(Map::new()));

                    relationships.push(DbEdge {
                        source: src_node.id,
                        target: tgt_node.id,
                        rel_type,
                        properties: props,
                        ..Default::default()
                    });
                }
            }
        }
    }

    let node_id_vec: Vec<String> = node_ids.into_iter().collect();
    let nodes = get_nodes_by_ids(exec, &node_id_vec)?;

    Ok((nodes, relationships))
}

pub fn insert_nodes(exec: &impl CypherExecutor, nodes: &[DbNode]) -> Result<usize> {
    if nodes.is_empty() {
        return Ok(0);
    }

    // Group nodes by label for efficient batching
    let mut nodes_by_label: std::collections::HashMap<String, Vec<&DbNode>> =
        std::collections::HashMap::new();
    for node in nodes {
        nodes_by_label
            .entry(node.label.clone())
            .or_default()
            .push(node);
    }

    // Batch insert using UNWIND with flattened properties.
    // MERGE on :Base label so the Base.objectid index is used for fast lookups.
    for (cypher_label, label_nodes) in nodes_by_label {
        for chunk in label_nodes.chunks(BATCH_SIZE) {
            let items: Vec<String> = chunk
                .iter()
                .map(|n| {
                    let flat_props = n.flatten_properties(true);
                    json_to_cypher_props(&flat_props, CypherEscapeStyle::Backslash)
                })
                .collect();

            let cypher = format!(
                "UNWIND [{}] AS props \
                 MERGE (n:Base {{objectid: props.objectid}}) \
                 SET n:{}, n += props \
                 REMOVE n.placeholder",
                items.join(", "),
                cypher_label
            );

            exec.exec_write(&cypher)?;
        }
    }

    Ok(nodes.len())
}
