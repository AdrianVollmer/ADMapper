# Tiering Model

ADMapper implements an enterprise tiering model to classify Active Directory
objects by privilege level. The goal is to identify relationships that cross
tier boundaries and could allow privilege escalation.

## Overview

The Enterprise Access Model divides AD objects into tiers based on their
criticality:

| Tier | Name | Examples |
|------|------|----------|
| **0** | Identity infrastructure | Domain Controllers, Domain Admins, Enterprise Admins, Schema Admins, PKI servers |
| **1** | Servers and enterprise apps | Member servers, application servers, service accounts |
| **2** | Workstations | End-user workstations, Domain Computers members |
| **3** | Standard users | Domain Users members and other unclassified objects |

Lower tier numbers represent higher privilege.

## Automatic Tier Assignment

After importing BloodHound data, ADMapper automatically assigns tiers to nodes
based on their well-known group membership. All assignments use the RID suffix
of the group's SID for reliable identification.

The following are applied in priority order. Once a tier is assigned, it is
never overwritten by a lower-priority rule.

### Tier 0 — Privileged groups and their members

The group object itself and all direct members of each well-known privileged
group receive tier 0:

| Group | RID |
|-------|-----|
| Domain Admins | -512 |
| Domain Controllers | -516 |
| Cert Publishers | -517 |
| Schema Admins | -518 |
| Enterprise Admins | -519 |
| Group Policy Creator Owners | -520 |
| Read-Only Domain Controllers | -521 |
| Protected Users | -525 |
| Key Admins | -526 |
| Enterprise Key Admins | -527 |
| Administrators (Builtin) | -544 |
| Account Operators | -548 |
| Server Operators | -549 |
| Print Operators | -550 |
| Backup Operators | -551 |
| Enterprise Read-Only DCs | -498 |
| Enterprise Domain Controllers | S-1-5-9 |

### Tier 2 — Domain Computers and their members

The Domain Computers group object itself (RID `-515`) and all its direct
members receive tier 2.

### Tier 3 — Domain Users members

Direct members of Domain Users (RID `-513`) receive tier 3.

### Ungrouped objects

Objects that do not match any of the above rules receive no automatic tier
assignment. Tier 1 assets (member servers, application servers, etc.) must be
classified manually using the **Edit Tiers** modal.

!!! note
    Exchange groups such as "Exchange Windows Permissions" and "Exchange Trusted
    Subsystem" do not have fixed well-known RIDs and therefore cannot be
    automatically classified. Assign them manually.

### Hypervisors and physical infrastructure

ESXi hosts, Hyper-V servers, and similar hypervisor platforms are not part of
Active Directory's object model and therefore never receive an automatic tier.
You are responsible for assigning the correct tier manually.

The critical rule: **a hypervisor inherits the tier of its most sensitive
guest.** If a host runs a domain controller or any other tier 0 system, the
hypervisor itself must be tier 0 — an attacker with hypervisor-level access can
trivially compromise every VM running on it.

To make these relationships visible in the graph, add manual **"Hosts"** edges
from each hypervisor node to the VMs it runs. Once those edges exist, tier
violations (e.g. a tier 1 hypervisor with a tier 0 guest) surface automatically
in the tier-violation view.

## Assigning Tiers Manually

Open the **Edit Tiers** modal from the toolbar to assign tiers to nodes.

### Filter Modes

#### Name Regex

Use a regular expression to match node names:

- `ADMIN` — matches all nodes with "ADMIN" in the name
- `^DC\d+` — matches nodes starting with "DC" followed by digits
- `SERVER|SRV` — matches nodes containing "SERVER" or "SRV"

#### Node Type

Filter by AD object type (User, Group, Computer, OU, Domain, GPO, etc.).

#### Group Membership

Search for a group and assign a tier to all its transitive members via
`MemberOf` relationships.

#### OU Containment

Search for an Organizational Unit and assign a tier to all objects it
recursively contains via `Contains` relationships.

#### Tag Visible Nodes

Assign a tier to all nodes currently visible in the graph. Useful after
running a query that surfaces a specific set of objects.

## Analyzing Tier Violations

Use the **Tier Analysis** built-in queries (in the sidebar query panel) to
find single-hop relationships that cross tier boundaries:

| Query | Description |
|-------|-------------|
| Tier 1 → Tier 0 | Tier 1 nodes with a direct relationship to tier 0 |
| Tier 2 → Tier 0 | Tier 2 nodes with a direct relationship to tier 0 |
| Tier 3 → Tier 0 | Tier 3 nodes with a direct relationship to tier 0 |
| Any → Tier 0 | All nodes reaching tier 0 in one hop |
| Tier 2 → Tier 1 | Tier 2 nodes with a direct relationship to tier 1 |
| Tier 3 → Tier 1 | Tier 3 nodes with a direct relationship to tier 1 |
| Any → Tier 1 | All nodes reaching tier 1 in one hop |
| Tier 3 → Tier 2 | Tier 3 nodes with a direct relationship to tier 2 |
| All Cross-Tier | All single-hop relationships where source tier > target tier |

The fine-grained queries (e.g. Tier 3 → Tier 0) are useful in large
environments where the broad "Any" queries return too many results.

### Remediation

Common remediation steps for tier violations:

- Remove unnecessary group memberships that span tiers
- Restrict delegation permissions that cross tier boundaries
- Separate service accounts by tier
- Review and remove stale ACL entries
- Implement proper tiered administration (separate admin accounts per tier)
