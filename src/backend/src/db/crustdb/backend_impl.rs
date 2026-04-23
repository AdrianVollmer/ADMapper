//! DatabaseBackend trait implementation for CrustDatabase.

use serde_json::Value as JsonValue;

use super::super::backend::{DatabaseBackend, QueryLanguage};
use super::super::types::{
    admin_types_set, DbEdge, DbNode, DetailedStats, Result, SecurityInsights,
};
use super::CrustDatabase;

impl DatabaseBackend for CrustDatabase {
    fn name(&self) -> &'static str {
        "CrustDB"
    }

    fn supports_language(&self, lang: QueryLanguage) -> bool {
        matches!(lang, QueryLanguage::Cypher)
    }

    fn default_language(&self) -> QueryLanguage {
        QueryLanguage::Cypher
    }

    fn ping(&self) -> Result<()> {
        self.run_custom_query("RETURN 1")?;
        Ok(())
    }

    fn clear(&self) -> Result<()> {
        CrustDatabase::clear(self)
    }

    fn insert_node(&self, node: DbNode) -> Result<()> {
        CrustDatabase::insert_node(self, node)
    }

    fn insert_edge(&self, relationship: DbEdge) -> Result<()> {
        CrustDatabase::insert_edge(self, relationship)
    }

    fn insert_nodes(&self, nodes: &[DbNode]) -> Result<usize> {
        CrustDatabase::insert_nodes(self, nodes)
    }

    fn insert_edges(&self, relationships: &[DbEdge]) -> Result<usize> {
        CrustDatabase::insert_edges(self, relationships)
    }

    fn get_stats(&self) -> Result<(usize, usize)> {
        CrustDatabase::get_stats(self)
    }

    fn get_detailed_stats(&self) -> Result<DetailedStats> {
        CrustDatabase::get_detailed_stats(self)
    }

    fn get_security_insights(&self) -> Result<SecurityInsights> {
        CrustDatabase::get_security_insights(self)
    }

    // get_choke_points: uses default trait implementation (algorithms.rs)
    // which loads all nodes/edges once and runs Brandes' algorithm in-memory.

    fn get_all_nodes(&self) -> Result<Vec<DbNode>> {
        CrustDatabase::get_all_nodes(self)
    }

    fn get_all_edges(&self) -> Result<Vec<DbEdge>> {
        CrustDatabase::get_all_edges(self)
    }

    fn get_nodes_by_ids(&self, ids: &[String]) -> Result<Vec<DbNode>> {
        CrustDatabase::get_nodes_by_ids(self, ids)
    }

    fn get_edges_between(&self, node_ids: &[String]) -> Result<Vec<DbEdge>> {
        CrustDatabase::get_edges_between(self, node_ids)
    }

    fn get_relationship_types(&self) -> Result<Vec<String>> {
        CrustDatabase::get_relationship_types(self)
    }

    fn get_node_types(&self) -> Result<Vec<String>> {
        CrustDatabase::get_node_types(self)
    }

    fn search_nodes(&self, query: &str, limit: usize, label: Option<&str>) -> Result<Vec<DbNode>> {
        CrustDatabase::search_nodes(self, query, limit, label)
    }

    fn resolve_node_identifier(&self, identifier: &str) -> Result<Option<String>> {
        CrustDatabase::resolve_node_identifier(self, identifier)
    }

    fn get_node_connections(
        &self,
        node_id: &str,
        direction: &str,
    ) -> Result<(Vec<DbNode>, Vec<DbEdge>)> {
        CrustDatabase::get_node_connections(self, node_id, direction)
    }

    fn get_node_relationship_counts(
        &self,
        node_id: &str,
    ) -> Result<(usize, usize, usize, usize, usize)> {
        // Get all relationships for this node efficiently
        let relationships = CrustDatabase::get_node_edges(self, node_id)?;

        let admin_types = admin_types_set();

        // Count unique nodes, not relationships
        // e.g., if node A has 3 relationships from node B, count as 1 incoming node
        let mut incoming_nodes: std::collections::HashSet<&str> = std::collections::HashSet::new();
        let mut outgoing_nodes: std::collections::HashSet<&str> = std::collections::HashSet::new();
        let mut admin_to_nodes: std::collections::HashSet<&str> = std::collections::HashSet::new();
        let mut member_of_nodes: std::collections::HashSet<&str> = std::collections::HashSet::new();
        let mut member_nodes: std::collections::HashSet<&str> = std::collections::HashSet::new();

        for relationship in &relationships {
            if relationship.target == node_id {
                incoming_nodes.insert(&relationship.source);
                if relationship.rel_type == "MemberOf" {
                    member_nodes.insert(&relationship.source);
                }
            }
            if relationship.source == node_id {
                outgoing_nodes.insert(&relationship.target);
                if relationship.rel_type == "MemberOf" {
                    member_of_nodes.insert(&relationship.target);
                }
                if admin_types.contains(relationship.rel_type.as_str()) {
                    admin_to_nodes.insert(&relationship.target);
                }
            }
        }

        Ok((
            incoming_nodes.len(),
            outgoing_nodes.len(),
            admin_to_nodes.len(),
            member_of_nodes.len(),
            member_nodes.len(),
        ))
    }

    fn is_member_of(&self, node_id: &str, target_id: &str) -> Result<bool> {
        // BFS over MemberOf relationships using per-node edge lookups
        let mut visited = std::collections::HashSet::new();
        let mut queue = std::collections::VecDeque::new();
        queue.push_back(node_id.to_string());
        visited.insert(node_id.to_string());

        while let Some(current) = queue.pop_front() {
            if current == target_id {
                return Ok(true);
            }
            let edges = CrustDatabase::get_node_edges(self, &current)?;
            for edge in &edges {
                if edge.source == current
                    && edge.rel_type == "MemberOf"
                    && !visited.contains(&edge.target)
                {
                    visited.insert(edge.target.clone());
                    queue.push_back(edge.target.clone());
                }
            }
        }

        Ok(false)
    }

    fn find_membership_by_sid_suffix(
        &self,
        node_id: &str,
        sid_suffix: &str,
    ) -> Result<Option<String>> {
        CrustDatabase::find_membership_by_sid_suffix(self, node_id, sid_suffix)
    }

    fn shortest_path(&self, from: &str, to: &str) -> Result<Option<Vec<(String, Option<String>)>>> {
        CrustDatabase::shortest_path(self, from, to)
    }

    fn find_paths_to_domain_admins(
        &self,
        exclude_relationship_types: &[String],
    ) -> Result<Vec<(String, String, String, usize)>> {
        CrustDatabase::find_paths_to_domain_admins(self, exclude_relationship_types)
    }

    fn update_exploit_likelihoods(
        &self,
        likelihoods: &std::collections::HashMap<String, f64>,
    ) -> Result<usize> {
        self.db
            .update_relationship_property_by_types("exploit_likelihood", likelihoods)
            .map_err(|e| super::super::types::DbError::Database(e.to_string()))
    }

    fn run_custom_query(&self, query: &str) -> Result<JsonValue> {
        CrustDatabase::run_custom_query(self, query)
    }

    fn get_cache_stats(&self) -> Result<Option<(usize, usize)>> {
        let stats = self.db.cache_stats()?;
        Ok(Some((stats.entry_count, stats.total_size_bytes)))
    }

    fn clear_cache(&self) -> Result<bool> {
        self.db.clear_cache()?;
        Ok(true)
    }

    fn get_database_size(&self) -> Result<Option<usize>> {
        let size = self.db.database_size()?;
        Ok(Some(size))
    }
}
