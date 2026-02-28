<div style="text-align: center; margin-bottom: 2rem;">
  <img src="assets/favicon.svg" alt="ADMapper" style="width: 96px; height: 96px;">
</div>

# ADMapper

ADMapper is an interactive graph visualization tool for Active Directory security analysis. It imports BloodHound collection data and provides a fast, intuitive interface for exploring AD permissions, finding attack paths, and identifying security weaknesses.

## Features

- **Fast Graph Rendering**: GPU-accelerated visualization handles large AD environments
- **Multiple Database Backends**: CrustDB (embedded), Neo4j, FalkorDB
- **Cypher Queries**: Run custom queries to explore the permission graph
- **Security Insights**: Automatic detection of high-value targets, kerberoastable accounts, and choke points
- **Desktop and Headless Modes**: Run as a native app or deploy as a web service
- **BloodHound Compatible**: Import data collected by SharpHound or BloodHound.py

## Quick Start

### Desktop Mode

Download the latest release for your platform and run the application. Import your BloodHound ZIP file using the import dialog.

### Headless Mode

Run ADMapper as a web server:

```bash
admapper --headless --port 9191 crustdb://./data.db
```

Then open `http://localhost:9191` in your browser.

## Node Types

ADMapper visualizes the following Active Directory objects:

| Type | Description |
|------|-------------|
| User | User accounts |
| Group | Security and distribution groups |
| Computer | Domain-joined computers |
| Domain | AD domains and trusts |
| GPO | Group Policy Objects |
| OU | Organizational Units |
| Container | AD containers |
| CertTemplate | Certificate templates |
| EnterpriseCA | Enterprise Certificate Authorities |

## Edge Types

Relationships between objects include:

| Category | Edge Types |
|----------|------------|
| Membership | MemberOf, Contains, LocalGroupMember |
| Sessions | HasSession, CanRDP, CanPSRemote |
| Dangerous | AdminTo, GenericAll, Owns, GenericWrite, WriteDacl |
| Delegation | AllowedToDelegate, TrustedBy |
| DCSync | DCSync, GetChanges, GetChangesAll |
| Credentials | ReadLAPSPassword, ReadGMSAPassword |

## License

MIT
