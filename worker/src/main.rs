mod aisstream;
mod backoff;
mod cli;
mod finland;
mod ingest;
mod pump;
mod queue;
mod stats;
mod upstream;

#[cfg(test)]
mod pipeline_test;

use anyhow::{bail, Result};
use clap::Parser;
use tracing_subscriber::EnvFilter;

use cli::{Cli, Command, ConnectArgs, ForwardArgs, SinkArgs};

#[tokio::main]
async fn main() -> Result<()> {
    // tokio-tungstenite links rustls without picking a crypto provider; install
    // one process-wide before any TLS handshake. Ignore the error that means it
    // was already installed.
    let _ = rustls::crypto::ring::default_provider().install_default();

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    match Cli::parse().command {
        Command::Connect(args) => run_connect(args).await,
        Command::Forward(args) => run_forward(args).await,
        Command::Rf | Command::Join => {
            eprintln!(
                "coming in milestone 3 — RF reception with SDR autodetect and antenna health checks"
            );
            std::process::exit(2);
        }
    }
}

async fn run_connect(args: ConnectArgs) -> Result<()> {
    // Denmark without a configured endpoint is a documented "come back once
    // you have access" case, not a generic error: exit 2 with instructions.
    if args.upstream == "denmark" && std::env::var_os(cli::DENMARK_ADDR_ENV).is_none() {
        eprintln!("{}", cli::DENMARK_HELP);
        std::process::exit(2);
    }
    if args.upstream == "aisstream" && std::env::var_os(aisstream::AISSTREAM_KEY_ENV).is_none() {
        eprintln!("{}", aisstream::AISSTREAM_HELP);
        std::process::exit(2);
    }
    let upstream = cli::parse_upstream(&args.upstream)?;
    let target = cli::parse_ingest(&args.sink.ingest)?;
    let key = require_key(&args.sink, &target)?;
    let source_label = args
        .sink
        .source
        .clone()
        .unwrap_or_else(|| cli::default_source_connect(&args.upstream));

    pump::run(pump::Source::Network(upstream), target, key, source_label).await
}

async fn run_forward(args: ForwardArgs) -> Result<()> {
    let target = cli::parse_ingest(&args.sink.ingest)?;
    let key = require_key(&args.sink, &target)?;

    let (source, default_label) = match (&args.listen_udp, &args.replay) {
        (Some(_), Some(_)) => bail!("use only one of --listen-udp or --replay"),
        (None, None) => bail!("forward needs one of --listen-udp or --replay"),
        (Some(addr), None) => (
            pump::Source::ListenUdp(addr.clone()),
            format!("forward:udp:{addr}"),
        ),
        (None, Some(path)) => {
            let label = format!("forward:replay:{}", path.display());
            (
                pump::Source::Replay {
                    path: path.clone(),
                    loop_forever: args.loop_,
                    rate: args.rate,
                },
                label,
            )
        }
    };

    let source_label = args.sink.source.clone().unwrap_or(default_label);
    pump::run(source, target, key, source_label).await
}

/// Ingest that is not UDP must carry an authentication key.
fn require_key(sink: &SinkArgs, target: &cli::Ingest) -> Result<Option<String>> {
    if !target.is_udp() && sink.key.is_none() {
        bail!("--key is required unless ingest is udp://");
    }
    Ok(sink.key.clone())
}
