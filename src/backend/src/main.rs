#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "admapper")]
#[command(about = "BloodHound frontend for AD permissions visualization")]
struct Args {
    /// Database URL to connect to on startup
    ///
    /// Supported formats:
    /// - crustdb:///path/to/file.db (CrustDB, file-based)
    /// - neo4j://[user:pass@]host[:port] (Neo4j, network)
    /// - bolt://[user:pass@]host[:port] (Neo4j, network)
    /// - falkordb://[user:pass@]host[:port] (FalkorDB, network)
    #[arg(index = 1)]
    database_url: Option<String>,

    /// Run as a headless web server instead of desktop app
    #[arg(long)]
    headless: bool,

    /// Port for service mode (default: 9191)
    #[arg(long, default_value = "9191")]
    port: u16,

    /// Bind address for service mode (default: 127.0.0.1)
    #[arg(long, default_value = "127.0.0.1")]
    bind: String,
}

fn main() {
    let args = Args::parse();

    if args.headless {
        admapper::run_service(&args.bind, args.port, args.database_url.as_deref());
    } else {
        admapper::run_desktop(args.database_url.as_deref());
    }
}
