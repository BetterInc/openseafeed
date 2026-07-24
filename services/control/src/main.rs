//! Binary entry point for the OpenSeaFeed control plane. All logic lives in
//! the library crate so integration tests can drive the router directly.

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    openseafeed_control::run().await
}
