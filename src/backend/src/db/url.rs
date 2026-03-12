//! Database URL parsing for multi-database support.
//!
//! Supports parsing URLs for different database backends:
//! - Neo4j: `neo4j://user:pass@host:7687` or `bolt://...`
//! - FalkorDB: `falkordb://user:pass@host:6379`
//! - CrustDB: `crustdb:///path/to/file.db`

use std::path::PathBuf;
use thiserror::Error;

/// Database type enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseType {
    /// Neo4j graph database (network)
    Neo4j,
    /// FalkorDB (Redis-based graph, network)
    FalkorDB,
    /// CrustDB (file-based, Cypher)
    CrustDB,
}

impl DatabaseType {
    /// Get a human-readable name for this database type.
    pub fn name(&self) -> &'static str {
        match self {
            DatabaseType::Neo4j => "Neo4j",
            DatabaseType::FalkorDB => "FalkorDB",
            DatabaseType::CrustDB => "CrustDB",
        }
    }

    /// Check if this database type requires a network connection.
    pub fn is_network(&self) -> bool {
        matches!(self, DatabaseType::Neo4j | DatabaseType::FalkorDB)
    }

    /// Check if this database type uses file storage.
    pub fn is_file(&self) -> bool {
        matches!(self, DatabaseType::CrustDB)
    }

    /// Get the default port for network databases.
    pub fn default_port(&self) -> Option<u16> {
        match self {
            DatabaseType::Neo4j => Some(7687),
            DatabaseType::FalkorDB => Some(6379),
            DatabaseType::CrustDB => None,
        }
    }
}

/// Parsed database URL.
// Fields are used by feature-gated backends (neo4j, falkordb).
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct DatabaseUrl {
    /// Type of database.
    pub db_type: DatabaseType,
    /// Host for network databases.
    pub host: Option<String>,
    /// Port for network databases.
    pub port: Option<u16>,
    /// Username for authentication.
    pub username: Option<String>,
    /// Password for authentication.
    pub password: Option<String>,
    /// Path for file-based databases.
    pub path: Option<PathBuf>,
    /// Database name (for Neo4j).
    pub database: Option<String>,
    /// Whether SSL/TLS should be used (for Neo4j +s/+ssc schemes).
    pub use_ssl: bool,
}

/// Error type for URL parsing.
#[derive(Error, Debug)]
pub enum ParseError {
    #[error("Unknown URL scheme: {0}")]
    UnknownScheme(String),
    #[error("Missing host for network database")]
    MissingHost,
    #[error("Missing path for file database")]
    MissingPath,
    #[error("Invalid URL format: {0}")]
    InvalidFormat(String),
    #[error("Invalid port number: {0}")]
    InvalidPort(String),
}

impl DatabaseUrl {
    /// Parse a database URL string.
    ///
    /// Supported formats:
    /// - `neo4j://[user:pass@]host[:port][/database]`
    /// - `bolt://[user:pass@]host[:port][/database]`
    /// - `falkordb://[user:pass@]host[:port]`
    /// - `crustdb:///path/to/file.db`
    pub fn parse(url: &str) -> Result<Self, ParseError> {
        // Extract scheme
        let (scheme, rest) = url
            .split_once("://")
            .ok_or_else(|| ParseError::InvalidFormat("Missing :// separator".to_string()))?;

        let scheme_lower = scheme.to_lowercase();

        // Check for SSL schemes (+s = SSL, +ssc = SSL with self-signed cert)
        let use_ssl = scheme_lower.ends_with("+s") || scheme_lower.ends_with("+ssc");

        let db_type = match scheme_lower.as_str() {
            "neo4j" | "bolt" | "neo4j+s" | "bolt+s" | "neo4j+ssc" | "bolt+ssc" => {
                DatabaseType::Neo4j
            }
            "falkordb" | "redis" => DatabaseType::FalkorDB,
            "crustdb" | "crust" => DatabaseType::CrustDB,
            other => return Err(ParseError::UnknownScheme(other.to_string())),
        };

        if db_type.is_file() {
            // File-based database: extract path
            let path_str = rest.trim_start_matches('/');
            if path_str.is_empty() {
                return Err(ParseError::MissingPath);
            }

            // For file paths, add leading / on Unix if it was absolute
            let path = if rest.starts_with('/') {
                PathBuf::from(format!("/{}", path_str))
            } else {
                PathBuf::from(path_str)
            };

            Ok(DatabaseUrl {
                db_type,
                host: None,
                port: None,
                username: None,
                password: None,
                path: Some(path),
                database: None,
                use_ssl: false, // File-based databases don't use SSL
            })
        } else {
            // Network database: parse host, port, auth
            Self::parse_network_url(db_type, rest, use_ssl)
        }
    }

    /// Parse network database URL components.
    fn parse_network_url(
        db_type: DatabaseType,
        url_part: &str,
        use_ssl: bool,
    ) -> Result<Self, ParseError> {
        let mut username = None;
        let mut password = None;
        let host;
        let mut port = db_type.default_port();
        let mut database = None;

        // Check for auth: user:pass@host
        let (auth_or_host, path_part) = if let Some(idx) = url_part.find('/') {
            (&url_part[..idx], Some(&url_part[idx + 1..]))
        } else {
            (url_part, None)
        };

        let host_port = if let Some(at_idx) = auth_or_host.rfind('@') {
            // Has authentication
            let auth = &auth_or_host[..at_idx];
            if let Some(colon_idx) = auth.find(':') {
                username = Some(auth[..colon_idx].to_string());
                password = Some(auth[colon_idx + 1..].to_string());
            } else {
                username = Some(auth.to_string());
            }
            &auth_or_host[at_idx + 1..]
        } else {
            auth_or_host
        };

        // Parse host:port
        if let Some(colon_idx) = host_port.rfind(':') {
            // Check if it's IPv6 (contains multiple colons)
            if host_port.matches(':').count() > 1 {
                // IPv6 address - use the whole thing as host
                host = host_port.to_string();
            } else {
                host = host_port[..colon_idx].to_string();
                let port_str = &host_port[colon_idx + 1..];
                port = Some(
                    port_str
                        .parse()
                        .map_err(|_| ParseError::InvalidPort(port_str.to_string()))?,
                );
            }
        } else {
            host = host_port.to_string();
        }

        if host.is_empty() {
            return Err(ParseError::MissingHost);
        }

        // Handle database name in path
        if let Some(path) = path_part {
            if !path.is_empty() {
                database = Some(path.to_string());
            }
        }

        Ok(DatabaseUrl {
            db_type,
            host: Some(host),
            port,
            username,
            password,
            path: None,
            database,
            use_ssl,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_crustdb_url() {
        let url = DatabaseUrl::parse("crustdb:///path/to/db.sqlite").unwrap();
        assert_eq!(url.db_type, DatabaseType::CrustDB);
        assert_eq!(url.path, Some(PathBuf::from("/path/to/db.sqlite")));
        assert!(url.host.is_none());
    }

    #[test]
    fn test_parse_neo4j_simple() {
        let url = DatabaseUrl::parse("neo4j://localhost").unwrap();
        assert_eq!(url.db_type, DatabaseType::Neo4j);
        assert_eq!(url.host, Some("localhost".to_string()));
        assert_eq!(url.port, Some(7687));
        assert!(url.username.is_none());
        assert!(url.password.is_none());
    }

    #[test]
    fn test_parse_neo4j_with_auth() {
        let url = DatabaseUrl::parse("neo4j://neo4j:password@localhost:7687").unwrap();
        assert_eq!(url.db_type, DatabaseType::Neo4j);
        assert_eq!(url.host, Some("localhost".to_string()));
        assert_eq!(url.port, Some(7687));
        assert_eq!(url.username, Some("neo4j".to_string()));
        assert_eq!(url.password, Some("password".to_string()));
    }

    #[test]
    fn test_parse_neo4j_with_database() {
        let url = DatabaseUrl::parse("neo4j://localhost:7687/mydb").unwrap();
        assert_eq!(url.db_type, DatabaseType::Neo4j);
        assert_eq!(url.database, Some("mydb".to_string()));
    }

    #[test]
    fn test_parse_bolt_url() {
        let url = DatabaseUrl::parse("bolt://localhost:7687").unwrap();
        assert_eq!(url.db_type, DatabaseType::Neo4j);
    }

    #[test]
    fn test_parse_falkordb() {
        let url = DatabaseUrl::parse("falkordb://user:pass@redis.example.com:6380").unwrap();
        assert_eq!(url.db_type, DatabaseType::FalkorDB);
        assert_eq!(url.host, Some("redis.example.com".to_string()));
        assert_eq!(url.port, Some(6380));
        assert_eq!(url.username, Some("user".to_string()));
        assert_eq!(url.password, Some("pass".to_string()));
    }

    #[test]
    fn test_parse_invalid_scheme() {
        let result = DatabaseUrl::parse("mysql://localhost");
        assert!(matches!(result, Err(ParseError::UnknownScheme(_))));
    }

    #[test]
    fn test_parse_missing_host() {
        let result = DatabaseUrl::parse("neo4j://");
        assert!(matches!(result, Err(ParseError::MissingHost)));
    }

    #[test]
    fn test_parse_missing_path() {
        let result = DatabaseUrl::parse("crustdb://");
        assert!(matches!(result, Err(ParseError::MissingPath)));
    }

    #[test]
    fn test_database_type_properties() {
        assert!(DatabaseType::Neo4j.is_network());
        assert!(DatabaseType::FalkorDB.is_network());
        assert!(DatabaseType::CrustDB.is_file());

        assert_eq!(DatabaseType::Neo4j.default_port(), Some(7687));
        assert_eq!(DatabaseType::FalkorDB.default_port(), Some(6379));
        assert_eq!(DatabaseType::CrustDB.default_port(), None);
    }

    #[test]
    fn test_parse_neo4j_ssl_scheme() {
        let url = DatabaseUrl::parse("neo4j+s://localhost:7687").unwrap();
        assert_eq!(url.db_type, DatabaseType::Neo4j);
        assert_eq!(url.host, Some("localhost".to_string()));
        assert!(url.use_ssl);
    }

    #[test]
    fn test_parse_bolt_ssl_scheme() {
        let url = DatabaseUrl::parse("bolt+s://localhost:7687").unwrap();
        assert_eq!(url.db_type, DatabaseType::Neo4j);
        assert!(url.use_ssl);
    }

    #[test]
    fn test_parse_neo4j_ssc_scheme() {
        let url = DatabaseUrl::parse("neo4j+ssc://localhost:7687").unwrap();
        assert_eq!(url.db_type, DatabaseType::Neo4j);
        assert!(url.use_ssl);
    }

    #[test]
    fn test_parse_neo4j_no_ssl() {
        let url = DatabaseUrl::parse("neo4j://localhost:7687").unwrap();
        assert_eq!(url.db_type, DatabaseType::Neo4j);
        assert!(!url.use_ssl);
    }
}
