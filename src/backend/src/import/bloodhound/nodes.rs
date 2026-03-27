//! Node extraction, UAC flag expansion, tier assignment, and type normalization.

use super::{tier_zero_rids, uac_flags, BloodHoundImporter};
use crate::db::DbNode;
use serde_json::Value as JsonValue;

impl BloodHoundImporter {
    /// Extract a node from a BloodHound entity.
    pub(super) fn extract_node(&self, data_type: &str, entity: &JsonValue) -> Option<DbNode> {
        let id = entity
            .get("ObjectIdentifier")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())?;

        let mut properties = entity.get("Properties").cloned().unwrap_or(JsonValue::Null);

        // Ensure objectid (SID) is always present in properties
        // Some BloodHound exports don't include it, but we need it for queries
        if let Some(props) = properties.as_object_mut() {
            if !props.contains_key("objectid") {
                props.insert("objectid".to_string(), JsonValue::String(id.clone()));
            }

            // Expand useraccountcontrol into individual boolean properties
            Self::expand_uac_flags(props);

            // Assign tier based on SID (tier 0 for privileged groups)
            Self::assign_tier(props, &id);
        }

        let label = properties
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or(&id)
            .to_string();

        let node_type = self.normalize_type(data_type);

        // Domains are always tier 0
        if node_type == "Domain" {
            if let Some(props) = properties.as_object_mut() {
                if !props.contains_key("tier") {
                    props.insert("tier".to_string(), JsonValue::Number(0.into()));
                }
            }
        }

        Some(DbNode {
            id,
            name: label,
            label: node_type.to_string(),
            properties,
        })
    }

    /// Expand useraccountcontrol bitmask into individual boolean properties.
    /// Also converts the raw UAC value to hex format for display.
    pub(super) fn expand_uac_flags(props: &mut serde_json::Map<String, JsonValue>) {
        // Look for useraccountcontrol (case-insensitive)
        let uac_value = props
            .get("useraccountcontrol")
            .or_else(|| props.get("UserAccountControl"))
            .and_then(|v| v.as_i64());

        let Some(uac) = uac_value else {
            return;
        };

        // Convert to hex string for display (e.g., "0x10200")
        props.insert(
            "useraccountcontrol_hex".to_string(),
            JsonValue::String(format!("0x{:X}", uac)),
        );

        // Extract security-relevant flags as individual boolean properties
        // Only set properties if they don't already exist (BloodHound may have them)

        // enabled = NOT ACCOUNTDISABLE (most important flag)
        if !props.contains_key("enabled") {
            let enabled = (uac & uac_flags::ACCOUNTDISABLE) == 0;
            props.insert("enabled".to_string(), JsonValue::Bool(enabled));
        }

        // password_not_required - security risk
        if !props.contains_key("password_not_required") {
            let flag = (uac & uac_flags::PASSWD_NOTREQD) != 0;
            if flag {
                props.insert("password_not_required".to_string(), JsonValue::Bool(true));
            }
        }

        // password_never_expires - common misconfiguration
        if !props.contains_key("password_never_expires") {
            let flag = (uac & uac_flags::DONT_EXPIRE_PASSWORD) != 0;
            if flag {
                props.insert("password_never_expires".to_string(), JsonValue::Bool(true));
            }
        }

        // smartcard_required
        if !props.contains_key("smartcard_required") {
            let flag = (uac & uac_flags::SMARTCARD_REQUIRED) != 0;
            if flag {
                props.insert("smartcard_required".to_string(), JsonValue::Bool(true));
            }
        }

        // trusted_for_delegation - unconstrained delegation (high risk)
        if !props.contains_key("trusted_for_delegation") {
            let flag = (uac & uac_flags::TRUSTED_FOR_DELEGATION) != 0;
            if flag {
                props.insert("trusted_for_delegation".to_string(), JsonValue::Bool(true));
            }
        }

        // not_delegated - protected from delegation
        if !props.contains_key("not_delegated") {
            let flag = (uac & uac_flags::NOT_DELEGATED) != 0;
            if flag {
                props.insert("not_delegated".to_string(), JsonValue::Bool(true));
            }
        }

        // dont_require_preauth - AS-REP roastable (critical for attackers)
        if !props.contains_key("dont_require_preauth") {
            let flag = (uac & uac_flags::DONT_REQ_PREAUTH) != 0;
            if flag {
                props.insert("dont_require_preauth".to_string(), JsonValue::Bool(true));
            }
        }

        // password_expired
        if !props.contains_key("password_expired") {
            let flag = (uac & uac_flags::PASSWORD_EXPIRED) != 0;
            if flag {
                props.insert("password_expired".to_string(), JsonValue::Bool(true));
            }
        }

        // trusted_to_auth_for_delegation - constrained delegation with protocol transition
        if !props.contains_key("trusted_to_auth_for_delegation") {
            let flag = (uac & uac_flags::TRUSTED_TO_AUTH_FOR_DELEGATION) != 0;
            if flag {
                props.insert(
                    "trusted_to_auth_for_delegation".to_string(),
                    JsonValue::Bool(true),
                );
            }
        }

        // account_locked_out
        if !props.contains_key("account_locked_out") {
            let flag = (uac & uac_flags::LOCKOUT) != 0;
            if flag {
                props.insert("account_locked_out".to_string(), JsonValue::Bool(true));
            }
        }
    }

    /// Well-known RIDs that should receive tier 2.
    const TIER_TWO_RIDS: &'static [&'static str] = &[
        "-515", // Domain Computers
    ];

    /// Assign tier based on the object's SID.
    /// Sets tier=0 for privileged RIDs, tier=2 for well-known non-privileged groups.
    pub(super) fn assign_tier(props: &mut serde_json::Map<String, JsonValue>, objectid: &str) {
        // Skip if already assigned
        if props.contains_key("tier") {
            return;
        }

        // Check if the object's SID ends with a tier-0 RID
        let is_tier_zero = tier_zero_rids::ALL
            .iter()
            .any(|rid| objectid.ends_with(rid));

        if is_tier_zero {
            props.insert("tier".to_string(), JsonValue::Number(0.into()));
            return;
        }

        // Check if the object's SID ends with a tier-2 RID
        let is_tier_two = Self::TIER_TWO_RIDS
            .iter()
            .any(|rid| objectid.ends_with(rid));

        if is_tier_two {
            props.insert("tier".to_string(), JsonValue::Number(2.into()));
        }
    }

    /// Normalize BloodHound type name to standard format.
    pub(super) fn normalize_type(&self, data_type: &str) -> String {
        match data_type.to_lowercase().as_str() {
            "users" | "user" => "User",
            "groups" | "group" => "Group",
            "computers" | "computer" => "Computer",
            "domains" | "domain" => "Domain",
            "gpos" | "gpo" => "GPO",
            "ous" | "ou" => "OU",
            "containers" | "container" => "Container",
            "certtemplates" | "certtemplate" => "CertTemplate",
            "enterprisecas" | "enterpriseca" => "EnterpriseCA",
            "rootcas" | "rootca" => "RootCA",
            "aiacas" | "aiaca" => "AIACA",
            "ntauthstores" | "ntauthstore" => "NTAuthStore",
            _ => "Base", // Unknown types get "Base" label
        }
        .to_string()
    }
}
