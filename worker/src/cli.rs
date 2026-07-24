use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use clap::{Args, Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "openseafeed-worker", version, about = "OpenSeaFeed receiver worker")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Consume an upstream AIS source and push it to OpenSeaFeed ingest.
    Connect(ConnectArgs),
    /// Relay a local NMEA source (UDP or a replay file) to ingest.
    Forward(ForwardArgs),
    /// RF reception with SDR autodetect (future milestone).
    Rf,
    /// Join RF reception into the network (future milestone).
    Join,
}

#[derive(Args, Debug)]
pub struct ConnectArgs {
    /// tcp://host:port, ws://, wss://, or a preset name (norway).
    #[arg(long)]
    pub upstream: String,
    #[command(flatten)]
    pub sink: SinkArgs,
}

#[derive(Args, Debug)]
pub struct ForwardArgs {
    /// Receive NMEA datagrams on this address, e.g. 0.0.0.0:10120.
    #[arg(long, visible_alias = "nmea-udp-in")]
    pub listen_udp: Option<String>,
    /// Read NMEA lines from a file instead of listening.
    #[arg(long)]
    pub replay: Option<PathBuf>,
    /// Repeat the replay file forever.
    #[arg(long = "loop")]
    pub loop_: bool,
    /// Replay pacing in lines per second.
    #[arg(long, default_value_t = 500)]
    pub rate: u32,
    #[command(flatten)]
    pub sink: SinkArgs,
}

#[derive(Args, Debug)]
pub struct SinkArgs {
    /// udp://host:port, tcp://host:port, or ws://|wss://host[:port]/v1/ingest.
    #[arg(long)]
    pub ingest: String,
    /// Feed key (osf_feed_... or osf_stn_...). Required unless ingest is udp.
    #[arg(long)]
    pub key: Option<String>,
    /// Provenance label; defaults to one derived from the source.
    #[arg(long)]
    pub source: Option<String>,
}

/// A validated upstream AIS source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Upstream {
    Tcp { host: String, port: u16 },
    Ws { url: String },
    /// Digitraffic marine AIS (MQTT over WebSocket).
    Finland,
}

/// Env var naming the DMA-granted TCP endpoint for the `denmark` preset.
pub const DENMARK_ADDR_ENV: &str = "OSF_DENMARK_ADDR";

/// Shown when `denmark` is requested without a configured endpoint.
pub const DENMARK_HELP: &str = "denmark: DMA's live AIS stream has no public endpoint; \
access is granted per user. Request it at \
https://www.dma.dk/safety-at-sea/navigational-information/ais-data, then set \
OSF_DENMARK_ADDR=tcp://host:port and rerun.";

/// A validated OpenSeaFeed ingest target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ingest {
    Udp { addr: String },
    Tcp { addr: String },
    Ws { url: String },
}

impl Ingest {
    pub fn is_udp(&self) -> bool {
        matches!(self, Ingest::Udp { .. })
    }
}

pub fn parse_upstream(s: &str) -> Result<Upstream> {
    match s {
        "norway" => Ok(Upstream::Tcp {
            host: "153.44.253.27".to_string(),
            port: 5631,
        }),
        "finland" => Ok(Upstream::Finland),
        "denmark" => match std::env::var(DENMARK_ADDR_ENV) {
            Ok(addr) => parse_upstream(&addr),
            Err(_) => bail!("{DENMARK_HELP}"),
        },
        _ => {
            if let Some(rest) = s.strip_prefix("tcp://") {
                let (host, port) = split_host_port(rest)?;
                Ok(Upstream::Tcp { host, port })
            } else if s.starts_with("ws://") || s.starts_with("wss://") {
                Ok(Upstream::Ws { url: s.to_string() })
            } else {
                bail!("unrecognized upstream '{s}': expected tcp://host:port, ws://, wss://, or a preset (norway)")
            }
        }
    }
}

pub fn parse_ingest(s: &str) -> Result<Ingest> {
    if let Some(rest) = s.strip_prefix("udp://") {
        require_host_port(rest)?;
        Ok(Ingest::Udp {
            addr: rest.to_string(),
        })
    } else if let Some(rest) = s.strip_prefix("tcp://") {
        require_host_port(rest)?;
        Ok(Ingest::Tcp {
            addr: rest.to_string(),
        })
    } else if s.starts_with("ws://") || s.starts_with("wss://") {
        Ok(Ingest::Ws { url: s.to_string() })
    } else {
        bail!("unrecognized ingest '{s}': expected udp://, tcp://, ws://, or wss://")
    }
}

/// Append `key` as a query parameter to a WebSocket ingest URL.
pub fn ws_ingest_url_with_key(url: &str, key: &str) -> String {
    let sep = if url.contains('?') { '&' } else { '?' };
    format!("{url}{sep}key={key}")
}

pub fn default_source_connect(upstream_arg: &str) -> String {
    let short = ["tcp://", "ws://", "wss://"]
        .iter()
        .find_map(|p| upstream_arg.strip_prefix(p))
        .unwrap_or(upstream_arg);
    format!("connect:{short}")
}

fn split_host_port(s: &str) -> Result<(String, u16)> {
    let (host, port) = s
        .rsplit_once(':')
        .with_context(|| format!("expected host:port, got '{s}'"))?;
    let port: u16 = port
        .parse()
        .with_context(|| format!("invalid port in '{s}'"))?;
    if host.is_empty() {
        bail!("empty host in '{s}'");
    }
    Ok((host.to_string(), port))
}

fn require_host_port(s: &str) -> Result<()> {
    split_host_port(s).map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn norway_preset_resolves_to_tcp() {
        assert_eq!(
            parse_upstream("norway").unwrap(),
            Upstream::Tcp {
                host: "153.44.253.27".to_string(),
                port: 5631
            }
        );
    }

    #[test]
    fn finland_preset_resolves() {
        assert_eq!(parse_upstream("finland").unwrap(), Upstream::Finland);
    }

    #[test]
    fn denmark_preset_follows_env() {
        // Unset: error that explains how to request DMA access.
        std::env::remove_var(DENMARK_ADDR_ENV);
        let e = parse_upstream("denmark").unwrap_err().to_string();
        assert!(e.contains("dma.dk"), "{e}");

        // Set to a TCP endpoint: behaves like a raw tcp:// upstream.
        std::env::set_var(DENMARK_ADDR_ENV, "tcp://ais.example.dk:4001");
        assert_eq!(
            parse_upstream("denmark").unwrap(),
            Upstream::Tcp {
                host: "ais.example.dk".to_string(),
                port: 4001
            }
        );
        std::env::remove_var(DENMARK_ADDR_ENV);
    }

    #[test]
    fn parses_tcp_and_ws_upstreams() {
        assert_eq!(
            parse_upstream("tcp://example.org:5631").unwrap(),
            Upstream::Tcp {
                host: "example.org".to_string(),
                port: 5631
            }
        );
        assert_eq!(
            parse_upstream("wss://feed.example/v1/stream").unwrap(),
            Upstream::Ws {
                url: "wss://feed.example/v1/stream".to_string()
            }
        );
    }

    #[test]
    fn rejects_unknown_upstream_scheme() {
        assert!(parse_upstream("http://example.org").is_err());
        assert!(parse_upstream("tcp://noport").is_err());
    }

    #[test]
    fn parses_ingest_variants() {
        assert!(parse_ingest("udp://127.0.0.1:10110").unwrap().is_udp());
        assert_eq!(
            parse_ingest("tcp://ingest.example:9000").unwrap(),
            Ingest::Tcp {
                addr: "ingest.example:9000".to_string()
            }
        );
        assert!(matches!(
            parse_ingest("wss://ingest.example/v1/ingest").unwrap(),
            Ingest::Ws { .. }
        ));
        assert!(parse_ingest("ftp://nope").is_err());
    }

    #[test]
    fn ws_url_key_appends_correctly() {
        assert_eq!(
            ws_ingest_url_with_key("wss://h/v1/ingest", "osf_feed_x"),
            "wss://h/v1/ingest?key=osf_feed_x"
        );
        assert_eq!(
            ws_ingest_url_with_key("wss://h/v1/ingest?foo=1", "osf_feed_x"),
            "wss://h/v1/ingest?foo=1&key=osf_feed_x"
        );
    }

    #[test]
    fn source_label_strips_scheme() {
        assert_eq!(default_source_connect("norway"), "connect:norway");
        assert_eq!(
            default_source_connect("tcp://153.44.253.27:5631"),
            "connect:153.44.253.27:5631"
        );
    }
}
