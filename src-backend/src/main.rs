#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "admapper")]
#[command(about = "BloodHound frontend for AD permissions visualization")]
struct Args {
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
        admapper::run_service(&args.bind, args.port);
    } else {
        admapper::run_desktop();
    }
}
