//! Sample Active Directory data generator.
//!
//! Generates realistic AD structure with deterministic pseudo-random data.

use crate::api::types::GenerateSize;
use crate::db::{DbEdge, DbNode};
use rand::{rngs::StdRng, seq::SliceRandom, Rng, SeedableRng};
use serde_json::json;

/// Constant seed for deterministic generation.
const SEED: u64 = 0xAD_AAAA_2024;

/// Tier model tiers.
#[derive(Clone, Copy, Debug)]
#[allow(dead_code)]
enum Tier {
    Zero, // Domain Controllers, Domain Admins
    One,  // Servers, Server Admins
    Two,  // Workstations, regular users
}

/// Domain information.
#[allow(dead_code)]
struct Domain {
    name: String,
    netbios: String,
    sid: String,
    is_root: bool,
    forest_id: usize,
}

/// Data generator state.
pub struct Generator {
    rng: StdRng,
    nodes: Vec<DbNode>,
    relationships: Vec<DbEdge>,
    domains: Vec<Domain>,
    next_user_id: u32,
    next_computer_id: u32,
    next_group_id: u32,
}

impl Generator {
    /// Create a new generator.
    fn new() -> Self {
        Self {
            rng: StdRng::seed_from_u64(SEED),
            nodes: Vec::new(),
            relationships: Vec::new(),
            domains: Vec::new(),
            next_user_id: 1000,
            next_computer_id: 1000,
            next_group_id: 1000,
        }
    }

    /// Generate data based on size preset.
    pub fn generate(size: GenerateSize) -> (Vec<DbNode>, Vec<DbEdge>) {
        let mut gen = Generator::new();

        match size {
            GenerateSize::Small => gen.generate_small(),
            GenerateSize::Medium => gen.generate_medium(),
            GenerateSize::Large => gen.generate_large(),
        }

        (gen.nodes, gen.relationships)
    }

    /// Small dataset: single domain, basic structure.
    fn generate_small(&mut self) {
        // Create single domain
        self.add_domain("CORP", "corp.local", true, 0);

        let domain = &self.domains[0];
        let domain_sid = domain.sid.clone();

        // Create default groups
        self.create_default_groups(&domain_sid, 0);

        // Create 3 DCs
        for i in 1..=3 {
            self.add_domain_controller(&format!("DC{:02}", i), &domain_sid, 0);
        }

        // Create 5 servers (Tier 1)
        for i in 1..=5 {
            self.add_server(&format!("SRV{:02}", i), &domain_sid, 0);
        }

        // Create 20 workstations (Tier 2)
        for i in 1..=20 {
            self.add_workstation(&format!("WKS{:03}", i), &domain_sid, 0);
        }

        // Create 30 users
        self.create_users(&domain_sid, 30, 0);

        // Create OUs
        self.create_ous(&domain_sid, 0);

        // Create GPOs
        self.create_gpos(&domain_sid, 0);

        // Add some vulnerabilities
        self.add_vulnerabilities(&domain_sid, 2);
    }

    /// Medium dataset: forest with multiple domains.
    fn generate_medium(&mut self) {
        // Forest root domain
        self.add_domain("CORP", "corp.local", true, 0);
        // Child domains
        self.add_domain("US", "us.corp.local", false, 0);
        self.add_domain("EU", "eu.corp.local", false, 0);

        for i in 0..3 {
            let domain = &self.domains[i];
            let domain_sid = domain.sid.clone();
            let is_root = domain.is_root;

            self.create_default_groups(&domain_sid, i);

            // DCs per domain
            let dc_count = if is_root { 3 } else { 2 };
            for j in 1..=dc_count {
                self.add_domain_controller(&format!("DC{:02}", j), &domain_sid, i);
            }

            // Servers
            let srv_count = if is_root { 10 } else { 5 };
            for j in 1..=srv_count {
                self.add_server(&format!("SRV{:02}", j), &domain_sid, i);
            }

            // Workstations
            let wks_count = if is_root { 50 } else { 30 };
            for j in 1..=wks_count {
                self.add_workstation(&format!("WKS{:03}", j), &domain_sid, i);
            }

            // Users
            let user_count = if is_root { 100 } else { 60 };
            self.create_users(&domain_sid, user_count, i);

            self.create_ous(&domain_sid, i);
            self.create_gpos(&domain_sid, i);
        }

        // Add trust relationships
        self.add_forest_trusts(0);

        // Add vulnerabilities
        for i in 0..3 {
            let sid = self.domains[i].sid.clone();
            self.add_vulnerabilities(&sid, 3);
        }
    }

    /// Large dataset: multiple forests, full tier model.
    fn generate_large(&mut self) {
        // Primary forest
        self.add_domain("CONTOSO", "contoso.com", true, 0);
        self.add_domain("NA", "na.contoso.com", false, 0);
        self.add_domain("EMEA", "emea.contoso.com", false, 0);
        self.add_domain("APAC", "apac.contoso.com", false, 0);

        // Foreign forest
        self.add_domain("PARTNER", "partner.net", true, 1);

        let domain_count = self.domains.len();

        for i in 0..domain_count {
            let domain = &self.domains[i];
            let domain_sid = domain.sid.clone();
            let is_root = domain.is_root;
            let is_primary = domain.forest_id == 0;

            self.create_default_groups(&domain_sid, i);

            // DCs
            let dc_count = if is_root { 4 } else { 2 };
            for j in 1..=dc_count {
                self.add_domain_controller(&format!("DC{:02}", j), &domain_sid, i);
            }

            // Servers (Tier 1)
            let srv_count = if is_root {
                25
            } else if is_primary {
                15
            } else {
                10
            };
            for j in 1..=srv_count {
                self.add_server(&format!("SRV{:02}", j), &domain_sid, i);
            }

            // Workstations (Tier 2)
            let wks_count = if is_root {
                150
            } else if is_primary {
                100
            } else {
                50
            };
            for j in 1..=wks_count {
                self.add_workstation(&format!("WKS{:03}", j), &domain_sid, i);
            }

            // Users
            let user_count = if is_root {
                300
            } else if is_primary {
                200
            } else {
                100
            };
            self.create_users(&domain_sid, user_count, i);

            self.create_ous(&domain_sid, i);
            self.create_gpos(&domain_sid, i);
        }

        // Add trust relationships
        self.add_forest_trusts(0);

        // Add foreign forest trust
        self.add_foreign_forest_trust();

        // Add vulnerabilities
        for i in 0..domain_count {
            let sid = self.domains[i].sid.clone();
            let vuln_count = if i == 0 { 8 } else { 4 };
            self.add_vulnerabilities(&sid, vuln_count);
        }
    }

    /// Add a domain.
    fn add_domain(&mut self, netbios: &str, fqdn: &str, is_root: bool, forest_id: usize) {
        let domain_idx = self.domains.len();
        let sid = format!("S-1-5-21-{}-{}-{}", 1000 + domain_idx, 2000, 3000);

        let domain = Domain {
            name: fqdn.to_string(),
            netbios: netbios.to_string(),
            sid: sid.clone(),
            is_root,
            forest_id,
        };

        let domain_node = DbNode {
            id: sid.clone(),
            name: fqdn.to_uppercase(),
            label: "Domain".to_string(),
            properties: json!({
                "objectid": sid,
                "name": fqdn.to_uppercase(),
                "domain": fqdn.to_uppercase(),
                "distinguishedname": format!("DC={}", fqdn.replace('.', ",DC=")),
                "functionallevel": "2016",
            }),
        };

        self.nodes.push(domain_node);
        self.domains.push(domain);
    }

    /// Create default AD groups for a domain.
    fn create_default_groups(&mut self, domain_sid: &str, domain_idx: usize) {
        let domain = &self.domains[domain_idx];
        let domain_name = domain.name.to_uppercase();

        // Well-known groups with their RIDs
        let groups = [
            ("Domain Admins", 512, true, Tier::Zero),
            ("Domain Users", 513, false, Tier::Two),
            ("Domain Computers", 515, false, Tier::Two),
            ("Domain Controllers", 516, true, Tier::Zero),
            ("Enterprise Admins", 519, true, Tier::Zero), // Only in root
            ("Schema Admins", 518, true, Tier::Zero),     // Only in root
            ("Administrators", 544, true, Tier::Zero),
            ("Server Operators", 549, false, Tier::One),
            ("Account Operators", 548, false, Tier::One),
            ("Backup Operators", 551, false, Tier::One),
            ("Print Operators", 550, false, Tier::One),
            ("Remote Desktop Users", 555, false, Tier::Two),
        ];

        for (name, rid, high_value, _tier) in groups {
            // Enterprise/Schema Admins only in root domain
            if (rid == 519 || rid == 518) && !domain.is_root {
                continue;
            }

            let group_sid = format!("{}-{}", domain_sid, rid);

            let group = DbNode {
                id: group_sid.clone(),
                name: format!("{}@{}", name.to_uppercase(), domain_name),
                label: "Group".to_string(),
                properties: json!({
                    "objectid": group_sid,
                    "name": format!("{}@{}", name.to_uppercase(), domain_name),
                    "domain": domain_name,
                    "highvalue": high_value,
                    "admincount": high_value,
                }),
            };

            self.nodes.push(group);

            // Domain Admins is member of Administrators
            if rid == 512 {
                self.relationships.push(DbEdge {
                    source: group_sid.clone(),
                    target: format!("{}-544", domain_sid),
                    rel_type: "MemberOf".to_string(),
                    properties: json!({}),
                    ..Default::default()
                });
            }

            // Add AdminTo for Domain Admins on Domain Controllers
            if rid == 512 {
                self.relationships.push(DbEdge {
                    source: group_sid,
                    target: domain_sid.to_string(),
                    rel_type: "GenericAll".to_string(),
                    properties: json!({}),
                    ..Default::default()
                });
            }
        }

        // Enterprise Domain Controllers (well-known SID S-1-5-9, not domain-relative)
        // Only create once (in root domain) since it's forest-wide
        if domain.is_root {
            let edc_sid = format!("{}-S-1-5-9", domain_sid);
            self.nodes.push(DbNode {
                id: edc_sid.clone(),
                name: format!("ENTERPRISE DOMAIN CONTROLLERS@{}", domain_name),
                label: "Group".to_string(),
                properties: json!({
                    "objectid": edc_sid,
                    "name": format!("ENTERPRISE DOMAIN CONTROLLERS@{}", domain_name),
                    "domain": domain_name,
                    "highvalue": true,
                    "is_highvalue": true,
                    "admincount": true,
                }),
            });
        }

        // Create custom groups
        self.create_custom_groups(domain_sid, domain_idx);
    }

    /// Create custom organizational groups.
    fn create_custom_groups(&mut self, domain_sid: &str, domain_idx: usize) {
        let domain = &self.domains[domain_idx];
        let domain_name = domain.name.to_uppercase();

        let custom_groups = [
            ("IT-ADMINS", Tier::One),
            ("HELPDESK", Tier::Two),
            ("HR", Tier::Two),
            ("FINANCE", Tier::Two),
            ("ENGINEERING", Tier::Two),
            ("SALES", Tier::Two),
            ("MARKETING", Tier::Two),
            ("SERVER-ADMINS", Tier::One),
            ("WORKSTATION-ADMINS", Tier::One),
            ("SQL-ADMINS", Tier::One),
            ("EXCHANGE-ADMINS", Tier::One),
            ("VPN-USERS", Tier::Two),
        ];

        for (name, _tier) in custom_groups {
            let group_sid = format!("{}-{}", domain_sid, self.next_group_id);
            self.next_group_id += 1;

            let group = DbNode {
                id: group_sid.clone(),
                name: format!("{}@{}", name, domain_name),
                label: "Group".to_string(),
                properties: json!({
                    "objectid": group_sid,
                    "name": format!("{}@{}", name, domain_name),
                    "domain": domain_name,
                }),
            };

            self.nodes.push(group);
        }
    }

    /// Add a domain controller.
    fn add_domain_controller(&mut self, hostname: &str, domain_sid: &str, domain_idx: usize) {
        let domain = &self.domains[domain_idx];
        let fqdn = format!("{}.{}", hostname, domain.name);
        let computer_sid = format!("{}-{}", domain_sid, self.next_computer_id);
        self.next_computer_id += 1;

        let dc = DbNode {
            id: computer_sid.clone(),
            name: fqdn.to_uppercase(),
            label: "Computer".to_string(),
            properties: json!({
                "objectid": computer_sid,
                "name": fqdn.to_uppercase(),
                "domain": domain.name.to_uppercase(),
                "operatingsystem": "Windows Server 2022 Datacenter",
                "enabled": true,
                "highvalue": true,
                "unconstraineddelegation": true,
            }),
        };

        self.nodes.push(dc);

        // DC is member of Domain Controllers
        self.relationships.push(DbEdge {
            source: computer_sid.clone(),
            target: format!("{}-516", domain_sid),
            rel_type: "MemberOf".to_string(),
            properties: json!({}),
            ..Default::default()
        });

        // Domain Admins have GenericAll on DC
        self.relationships.push(DbEdge {
            source: format!("{}-512", domain_sid),
            target: computer_sid,
            rel_type: "GenericAll".to_string(),
            properties: json!({}),
            ..Default::default()
        });
    }

    /// Add a server (Tier 1).
    fn add_server(&mut self, hostname: &str, domain_sid: &str, domain_idx: usize) {
        let domain = &self.domains[domain_idx];
        let fqdn = format!("{}.{}", hostname, domain.name);
        let computer_sid = format!("{}-{}", domain_sid, self.next_computer_id);
        self.next_computer_id += 1;

        let os_options = [
            "Windows Server 2022 Standard",
            "Windows Server 2019 Standard",
            "Windows Server 2016 Standard",
        ];
        let os = os_options[self.rng.gen_range(0..os_options.len())];

        let server = DbNode {
            id: computer_sid.clone(),
            name: fqdn.to_uppercase(),
            label: "Computer".to_string(),
            properties: json!({
                "objectid": computer_sid,
                "name": fqdn.to_uppercase(),
                "domain": domain.name.to_uppercase(),
                "operatingsystem": os,
                "enabled": true,
            }),
        };

        self.nodes.push(server);

        // Server is member of Domain Computers
        self.relationships.push(DbEdge {
            source: computer_sid.clone(),
            target: format!("{}-515", domain_sid),
            rel_type: "MemberOf".to_string(),
            properties: json!({}),
            ..Default::default()
        });

        // Server Admins group has AdminTo on servers
        // Find Server Admins group
        let server_admins = self
            .nodes
            .iter()
            .find(|n| {
                n.label == "Group"
                    && n.name.starts_with("SERVER-ADMINS@")
                    && n.id.starts_with(domain_sid)
            })
            .map(|n| n.id.clone());

        if let Some(sa_id) = server_admins {
            self.relationships.push(DbEdge {
                source: sa_id,
                target: computer_sid,
                rel_type: "AdminTo".to_string(),
                properties: json!({}),
                ..Default::default()
            });
        }
    }

    /// Add a workstation (Tier 2).
    fn add_workstation(&mut self, hostname: &str, domain_sid: &str, domain_idx: usize) {
        let domain = &self.domains[domain_idx];
        let fqdn = format!("{}.{}", hostname, domain.name);
        let computer_sid = format!("{}-{}", domain_sid, self.next_computer_id);
        self.next_computer_id += 1;

        let os_options = ["Windows 11 Enterprise", "Windows 10 Enterprise"];
        let os = os_options[self.rng.gen_range(0..os_options.len())];

        let wks = DbNode {
            id: computer_sid.clone(),
            name: fqdn.to_uppercase(),
            label: "Computer".to_string(),
            properties: json!({
                "objectid": computer_sid,
                "name": fqdn.to_uppercase(),
                "domain": domain.name.to_uppercase(),
                "operatingsystem": os,
                "enabled": true,
            }),
        };

        self.nodes.push(wks);

        // Workstation is member of Domain Computers
        self.relationships.push(DbEdge {
            source: computer_sid.clone(),
            target: format!("{}-515", domain_sid),
            rel_type: "MemberOf".to_string(),
            properties: json!({}),
            ..Default::default()
        });

        // Workstation Admins group has AdminTo on workstations
        let wks_admins = self
            .nodes
            .iter()
            .find(|n| {
                n.label == "Group"
                    && n.name.starts_with("WORKSTATION-ADMINS@")
                    && n.id.starts_with(domain_sid)
            })
            .map(|n| n.id.clone());

        if let Some(wa_id) = wks_admins {
            self.relationships.push(DbEdge {
                source: wa_id,
                target: computer_sid,
                rel_type: "AdminTo".to_string(),
                properties: json!({}),
                ..Default::default()
            });
        }
    }

    /// Create users for a domain.
    fn create_users(&mut self, domain_sid: &str, count: usize, domain_idx: usize) {
        let domain = &self.domains[domain_idx];
        let domain_name = domain.name.to_uppercase();

        // First names and last names for generating realistic usernames
        let first_names = [
            "James",
            "Mary",
            "John",
            "Patricia",
            "Robert",
            "Jennifer",
            "Michael",
            "Linda",
            "David",
            "Elizabeth",
            "William",
            "Barbara",
            "Richard",
            "Susan",
            "Joseph",
            "Jessica",
            "Thomas",
            "Sarah",
            "Christopher",
            "Karen",
            "Charles",
            "Lisa",
            "Daniel",
            "Nancy",
            "Matthew",
            "Betty",
            "Anthony",
            "Margaret",
            "Mark",
            "Sandra",
            "Steven",
            "Ashley",
            "Paul",
            "Kimberly",
            "Andrew",
            "Emily",
            "Joshua",
            "Donna",
            "Kenneth",
            "Michelle",
        ];
        let last_names = [
            "Smith",
            "Johnson",
            "Williams",
            "Brown",
            "Jones",
            "Garcia",
            "Miller",
            "Davis",
            "Rodriguez",
            "Martinez",
            "Hernandez",
            "Lopez",
            "Gonzalez",
            "Wilson",
            "Anderson",
            "Thomas",
            "Taylor",
            "Moore",
            "Jackson",
            "Martin",
            "Lee",
            "Perez",
            "Thompson",
            "White",
            "Harris",
            "Sanchez",
            "Clark",
            "Ramirez",
            "Lewis",
            "Robinson",
            "Walker",
            "Young",
            "Allen",
            "King",
            "Wright",
            "Scott",
            "Torres",
            "Nguyen",
            "Hill",
            "Flores",
        ];

        // Department groups
        let departments = ["HR", "FINANCE", "ENGINEERING", "SALES", "MARKETING"];

        // Create a few admin users first
        let admin_count = (count as f64 * 0.05).ceil() as usize; // 5% admins
        let it_count = (count as f64 * 0.1).ceil() as usize; // 10% IT staff

        for i in 0..count {
            let first = first_names[self.rng.gen_range(0..first_names.len())];
            let last = last_names[self.rng.gen_range(0..last_names.len())];
            let username = format!("{}.{}", first.to_lowercase(), last.to_lowercase());
            let user_sid = format!("{}-{}", domain_sid, self.next_user_id);
            self.next_user_id += 1;

            let enabled = self.rng.gen_bool(0.95); // 95% enabled
            let pwd_never_expires = self.rng.gen_bool(0.1);

            let user = DbNode {
                id: user_sid.clone(),
                name: format!("{}@{}", username.to_uppercase(), domain_name),
                label: "User".to_string(),
                properties: json!({
                    "objectid": user_sid,
                    "name": format!("{}@{}", username.to_uppercase(), domain_name),
                    "displayname": format!("{} {}", first, last),
                    "domain": domain_name,
                    "enabled": enabled,
                    "pwdneverexpires": pwd_never_expires,
                    "lastlogon": if enabled { json!("2024-01-15T08:30:00Z") } else { json!(null) },
                }),
            };

            self.nodes.push(user);

            // All users are members of Domain Users
            self.relationships.push(DbEdge {
                source: user_sid.clone(),
                target: format!("{}-513", domain_sid),
                rel_type: "MemberOf".to_string(),
                properties: json!({}),
                ..Default::default()
            });

            // Assign to groups based on role
            if i < admin_count {
                // Domain Admin
                self.relationships.push(DbEdge {
                    source: user_sid.clone(),
                    target: format!("{}-512", domain_sid),
                    rel_type: "MemberOf".to_string(),
                    properties: json!({}),
                    ..Default::default()
                });
            } else if i < admin_count + it_count {
                // IT staff - add to various IT groups
                let it_groups = [
                    "IT-ADMINS",
                    "HELPDESK",
                    "SERVER-ADMINS",
                    "WORKSTATION-ADMINS",
                ];
                let group_name = it_groups[self.rng.gen_range(0..it_groups.len())];

                let it_group = self
                    .nodes
                    .iter()
                    .find(|n| {
                        n.label == "Group"
                            && n.name.starts_with(&format!("{}@", group_name))
                            && n.id.starts_with(domain_sid)
                    })
                    .map(|n| n.id.clone());

                if let Some(g_id) = it_group {
                    self.relationships.push(DbEdge {
                        source: user_sid.clone(),
                        target: g_id,
                        rel_type: "MemberOf".to_string(),
                        properties: json!({}),
                        ..Default::default()
                    });
                }
            } else {
                // Regular user - assign to department
                let dept = departments[self.rng.gen_range(0..departments.len())];
                let dept_group = self
                    .nodes
                    .iter()
                    .find(|n| {
                        n.label == "Group"
                            && n.name.starts_with(&format!("{}@", dept))
                            && n.id.starts_with(domain_sid)
                    })
                    .map(|n| n.id.clone());

                if let Some(g_id) = dept_group {
                    self.relationships.push(DbEdge {
                        source: user_sid.clone(),
                        target: g_id,
                        rel_type: "MemberOf".to_string(),
                        properties: json!({}),
                        ..Default::default()
                    });
                }
            }

            // Add sessions for some enabled users
            if enabled && self.rng.gen_bool(0.3) {
                // Find a workstation to have session on
                let workstations: Vec<String> = self
                    .nodes
                    .iter()
                    .filter(|n| {
                        n.label == "Computer"
                            && n.id.starts_with(domain_sid)
                            && n.properties
                                .get("operatingsystem")
                                .and_then(|v| v.as_str())
                                .map(|s| s.contains("Windows 10") || s.contains("Windows 11"))
                                .unwrap_or(false)
                    })
                    .map(|n| n.id.clone())
                    .collect();

                if !workstations.is_empty() {
                    let wks = workstations[self.rng.gen_range(0..workstations.len())].clone();
                    // HasSession: Computer -> User (computer has session of user)
                    self.relationships.push(DbEdge {
                        source: wks,
                        target: user_sid,
                        rel_type: "HasSession".to_string(),
                        properties: json!({}),
                        ..Default::default()
                    });
                }
            }
        }
    }

    /// Create organizational units.
    fn create_ous(&mut self, domain_sid: &str, domain_idx: usize) {
        let domain = &self.domains[domain_idx];
        let domain_name = domain.name.to_uppercase();
        let domain_dn = format!("DC={}", domain.name.replace('.', ",DC="));

        let ous = [
            ("Users", "Organizational unit for user accounts"),
            ("Computers", "Organizational unit for computer accounts"),
            ("Servers", "Organizational unit for server accounts"),
            (
                "Workstations",
                "Organizational unit for workstation accounts",
            ),
            ("Groups", "Organizational unit for security groups"),
            (
                "Service Accounts",
                "Organizational unit for service accounts",
            ),
            ("Admins", "Organizational unit for admin accounts"),
        ];

        for (name, desc) in ous {
            let ou_id = format!("{}-OU-{}", domain_sid, name.replace(' ', "-"));

            let ou = DbNode {
                id: ou_id.clone(),
                name: format!("{}@{}", name.to_uppercase(), domain_name),
                label: "OU".to_string(),
                properties: json!({
                    "objectid": ou_id,
                    "name": format!("{}@{}", name.to_uppercase(), domain_name),
                    "domain": domain_name,
                    "distinguishedname": format!("OU={},{}", name, domain_dn),
                    "description": desc,
                }),
            };

            self.nodes.push(ou);

            // Domain Contains OU
            self.relationships.push(DbEdge {
                source: domain_sid.to_string(),
                target: ou_id,
                rel_type: "Contains".to_string(),
                properties: json!({}),
                ..Default::default()
            });
        }
    }

    /// Create Group Policy Objects.
    fn create_gpos(&mut self, domain_sid: &str, domain_idx: usize) {
        let domain = &self.domains[domain_idx];
        let domain_name = domain.name.to_uppercase();

        let gpos = [
            ("Default Domain Policy", true),
            ("Default Domain Controllers Policy", true),
            ("Password Policy", false),
            ("Audit Policy", false),
            ("Software Restriction", false),
            ("Desktop Lockdown", false),
        ];

        for (name, is_default) in gpos {
            let gpo_guid = format!(
                "{{{:08X}-{:04X}-{:04X}-{:04X}-{:012X}}}",
                self.rng.gen::<u32>(),
                self.rng.gen::<u16>(),
                self.rng.gen::<u16>(),
                self.rng.gen::<u16>(),
                self.rng.gen::<u64>() & 0xFFFFFFFFFFFF
            );
            let gpo_id = format!("{}-GPO-{}", domain_sid, gpo_guid);

            let gpo = DbNode {
                id: gpo_id.clone(),
                name: format!("{}@{}", name.to_uppercase(), domain_name),
                label: "GPO".to_string(),
                properties: json!({
                    "objectid": gpo_id,
                    "name": format!("{}@{}", name.to_uppercase(), domain_name),
                    "domain": domain_name,
                    "gpcpath": format!("\\\\{}\\sysvol\\{}\\Policies\\{}", domain.name, domain.name, gpo_guid),
                    "highvalue": is_default,
                }),
            };

            self.nodes.push(gpo);

            // GPLink to domain
            self.relationships.push(DbEdge {
                source: gpo_id,
                target: domain_sid.to_string(),
                rel_type: "GPLink".to_string(),
                properties: json!({}),
                ..Default::default()
            });
        }
    }

    /// Add trust relationships within a forest.
    fn add_forest_trusts(&mut self, forest_id: usize) {
        let forest_domains: Vec<(String, bool)> = self
            .domains
            .iter()
            .filter(|d| d.forest_id == forest_id)
            .map(|d| (d.sid.clone(), d.is_root))
            .collect();

        // Find root domain
        let root_sid = forest_domains
            .iter()
            .find(|(_, r)| *r)
            .map(|(s, _)| s.clone());

        if let Some(root) = root_sid {
            // Child domains trust root
            for (child_sid, is_root) in &forest_domains {
                if !is_root {
                    self.relationships.push(DbEdge {
                        source: child_sid.clone(),
                        target: root.clone(),
                        rel_type: "TrustedBy".to_string(),
                        properties: json!({
                            "trusttype": "ParentChild",
                            "transitive": true,
                        }),
                        ..Default::default()
                    });
                }
            }
        }
    }

    /// Add foreign forest trust.
    fn add_foreign_forest_trust(&mut self) {
        let primary_root = self
            .domains
            .iter()
            .find(|d| d.forest_id == 0 && d.is_root)
            .map(|d| d.sid.clone());

        let foreign_root = self
            .domains
            .iter()
            .find(|d| d.forest_id == 1 && d.is_root)
            .map(|d| d.sid.clone());

        if let (Some(pr), Some(fr)) = (primary_root, foreign_root) {
            // Bidirectional external trust
            self.relationships.push(DbEdge {
                source: fr.clone(),
                target: pr.clone(),
                rel_type: "TrustedBy".to_string(),
                properties: json!({
                    "trusttype": "External",
                    "transitive": false,
                }),
                ..Default::default()
            });

            self.relationships.push(DbEdge {
                source: pr,
                target: fr,
                rel_type: "TrustedBy".to_string(),
                properties: json!({
                    "trusttype": "External",
                    "transitive": false,
                }),
                ..Default::default()
            });
        }
    }

    /// Add common vulnerabilities.
    fn add_vulnerabilities(&mut self, domain_sid: &str, count: usize) {
        // Collect potential sources and targets
        let regular_users: Vec<String> = self
            .nodes
            .iter()
            .filter(|n| {
                n.label == "User"
                    && n.id.starts_with(domain_sid)
                    && !self
                        .relationships
                        .iter()
                        .any(|e| e.source == n.id && e.target == format!("{}-512", domain_sid))
            })
            .map(|n| n.id.clone())
            .collect();

        let computers: Vec<String> = self
            .nodes
            .iter()
            .filter(|n| n.label == "Computer" && n.id.starts_with(domain_sid))
            .map(|n| n.id.clone())
            .collect();

        let groups: Vec<String> = self
            .nodes
            .iter()
            .filter(|n| n.label == "Group" && n.id.starts_with(domain_sid))
            .map(|n| n.id.clone())
            .collect();

        // Vulnerability patterns
        let vulnerability_types = [
            "GenericAll",   // Full control
            "WriteDacl",    // Can modify ACL
            "WriteOwner",   // Can take ownership
            "GenericWrite", // Can write to object
            "AddMember",    // Can add group members
            "AllExtendedRights",
            "ForceChangePassword",
        ];

        for _ in 0..count {
            if regular_users.is_empty() || (computers.is_empty() && groups.is_empty()) {
                break;
            }

            let source = regular_users[self.rng.gen_range(0..regular_users.len())].clone();

            // Choose target - either a computer or a privileged group
            let target = if self.rng.gen_bool(0.5) && !computers.is_empty() {
                computers[self.rng.gen_range(0..computers.len())].clone()
            } else if !groups.is_empty() {
                // Try to target IT groups for attack path
                let it_groups: Vec<&String> = groups
                    .iter()
                    .filter(|g| {
                        self.nodes
                            .iter()
                            .find(|n| n.id == **g)
                            .map(|n| {
                                n.name.contains("ADMIN")
                                    || n.name.contains("IT-")
                                    || n.name.contains("SERVER")
                            })
                            .unwrap_or(false)
                    })
                    .collect();

                if !it_groups.is_empty() && self.rng.gen_bool(0.7) {
                    it_groups[self.rng.gen_range(0..it_groups.len())].clone()
                } else {
                    groups[self.rng.gen_range(0..groups.len())].clone()
                }
            } else {
                continue;
            };

            let vuln_type = vulnerability_types[self.rng.gen_range(0..vulnerability_types.len())];

            // Check we don't already have this relationship
            let exists = self
                .relationships
                .iter()
                .any(|e| e.source == source && e.target == target && e.rel_type == vuln_type);

            if !exists {
                self.relationships.push(DbEdge {
                    source,
                    target,
                    rel_type: vuln_type.to_string(),
                    properties: json!({
                        "isacl": true,
                        "isinherited": self.rng.gen_bool(0.3),
                    }),
                    ..Default::default()
                });
            }
        }

        // Add a few Kerberoastable users
        let kerberoastable_count = (count as f64 * 0.3).ceil() as usize;
        let mut candidates = regular_users.clone();
        candidates.shuffle(&mut self.rng);

        for user_id in candidates.iter().take(kerberoastable_count) {
            if let Some(node) = self.nodes.iter_mut().find(|n| n.id == *user_id) {
                if let Some(props) = node.properties.as_object_mut() {
                    props.insert("hasspn".to_string(), json!(true));
                    props.insert(
                        "serviceprincipalnames".to_string(),
                        json!(["MSSQLSvc/sql.corp.local:1433"]),
                    );
                }
            }
        }

        // Add some DCSync rights to a non-DA user (dangerous misconfiguration)
        if !regular_users.is_empty() && self.rng.gen_bool(0.3) {
            let attacker = regular_users[self.rng.gen_range(0..regular_users.len())].clone();
            self.relationships.push(DbEdge {
                source: attacker.clone(),
                target: domain_sid.to_string(),
                rel_type: "GetChanges".to_string(),
                properties: json!({}),
                ..Default::default()
            });
            self.relationships.push(DbEdge {
                source: attacker,
                target: domain_sid.to_string(),
                rel_type: "GetChangesAll".to_string(),
                properties: json!({}),
                ..Default::default()
            });
        }
    }
}
