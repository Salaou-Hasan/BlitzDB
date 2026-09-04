use anyhow::Result;

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();
    tracing::info!("BlitzDB CLI starting");

    eprintln!("blitz-cli v0.1.0");
    eprintln!("BlitzDB - A general-purpose high-performance application database/runtime");
    Ok(())
}
