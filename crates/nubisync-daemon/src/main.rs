//! NubiSync background daemon entry point.

#![forbid(unsafe_code)]

use std::env;

fn main() {
    match env::args().nth(1).as_deref() {
        Some("--version" | "-V") => {
            println!("nubisyncd {}", env!("CARGO_PKG_VERSION"));
        }
        _ => {
            println!(
                "NubiSync daemon foundation {} (no synchronization loop enabled yet)",
                env!("CARGO_PKG_VERSION")
            );
        }
    }
}
