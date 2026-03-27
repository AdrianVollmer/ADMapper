//! Helper functions for mapping BloodHound ACE rights and local group names
//! to edge types.

use super::BloodHoundImporter;

impl BloodHoundImporter {
    /// Map local group name to relationship type.
    ///
    /// Returns `None` for unrecognized group names -- BH CE only creates edges
    /// for the well-known local group types, not a generic fallback.
    pub(super) fn local_group_to_edge_type(&self, group_name: &str) -> Option<&'static str> {
        let upper = group_name.to_uppercase();
        if upper.contains("ADMINISTRATORS") {
            Some("AdminTo")
        } else if upper.contains("REMOTE DESKTOP") {
            Some("CanRDP")
        } else if upper.contains("REMOTE MANAGEMENT") {
            Some("CanPSRemote")
        } else if upper.contains("DISTRIBUTED COM") {
            Some("ExecuteDCOM")
        } else if upper.contains("REMOTE INTERACTIVE LOGON") {
            Some("RemoteInteractiveLogonRight")
        } else {
            None
        }
    }

    /// Map an ACE right name to its BH CE edge type.
    ///
    /// Returns `None` for unrecognized rights -- BH CE never creates generic
    /// "ACE" edges; only specifically recognized rights produce edges.
    pub(super) fn ace_to_edge_type(&self, right_name: &str) -> Option<&'static str> {
        Some(match right_name {
            // Core AD permissions
            "GenericAll" => "GenericAll",
            "GenericWrite" => "GenericWrite",
            "WriteOwner" => "WriteOwner",
            "WriteDacl" => "WriteDacl",
            "Owns" => "Owns",
            "AddMember" => "AddMember",
            "AddSelf" => "AddSelf",
            "ForceChangePassword" => "ForceChangePassword",
            "AllExtendedRights" => "AllExtendedRights",
            "AddKeyCredentialLink" => "AddKeyCredentialLink",
            "AddAllowedToAct" => "AddAllowedToAct",
            "WriteSPN" => "WriteSPN",
            "WriteAccountRestrictions" => "WriteAccountRestrictions",
            // LAPS / gMSA / sMSA
            "ReadLAPSPassword" => "ReadLAPSPassword",
            "ReadGMSAPassword" => "ReadGMSAPassword",
            "SyncLAPSPassword" => "SyncLAPSPassword",
            "DumpSMSAPassword" => "DumpSMSAPassword",
            // DCSync components
            "GetChanges" => "GetChanges",
            "GetChangesAll" => "GetChangesAll",
            "GetChangesInFilteredSet" => "GetChangesInFilteredSet",
            // PKI / ADCS
            "Enroll" => "Enroll",
            "ManageCA" => "ManageCA",
            "ManageCertificates" => "ManageCertificates",
            "WritePKINameFlag" => "WritePKINameFlag",
            "WritePKIEnrollmentFlag" => "WritePKIEnrollmentFlag",
            "HostsCAService" => "HostsCAService",
            "DelegatedEnrollmentAgent" => "DelegatedEnrollmentAgent",
            _ => return None,
        })
    }
}
