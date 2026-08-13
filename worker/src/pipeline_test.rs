//! Integration-style test: fake TCP upstream -> pipeline -> fake UDP ingest.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, UdpSocket};

use crate::cli::{Ingest, Upstream};
use crate::queue::LineQueue;
use crate::stats::Stats;
use crate::{ingest, upstream};

const VALID: [&str; 3] = [
    "!AIVDM,1,1,,B,177KQJ5000G?tO`K>RA1wUbN0TKH,0*5C",
    "!AIVDM,2,1,3,B,55P5TL01VIaAL@7WKO@mBplU@<PDhh000000001S;AJ::4A80?4i@E53,0*3E",
    "!AIVDM,2,2,3,B,1@0000000000000,2*55",
];
// Same payload as the first line but with a deliberately wrong checksum.
const INVALID: &str = "!AIVDM,1,1,,B,177KQJ5000G?tO`K>RA1wUbN0TKH,0*5D";

#[tokio::test]
async fn forwards_only_valid_lines_end_to_end() {
    // Fake ingest: a UDP socket the pipeline will push datagrams to.
    let ingest_sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let ingest_addr = ingest_sock.local_addr().unwrap();

    // Fake upstream: a TCP listener that sends 3 valid + 1 invalid lines once.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        // Interleave the invalid line so ordering can't hide a bug.
        let lines = [VALID[0], INVALID, VALID[1], VALID[2]];
        for line in lines {
            sock.write_all(line.as_bytes()).await.unwrap();
            sock.write_all(b"\n").await.unwrap();
        }
        sock.flush().await.unwrap();
        // Hold the connection briefly so the reader drains before EOF.
        tokio::time::sleep(Duration::from_millis(200)).await;
    });

    let stats = Arc::new(Stats::default());
    let queue = Arc::new(LineQueue::new(10_000));

    let up = Upstream::Tcp {
        host: upstream_addr.ip().to_string(),
        port: upstream_addr.port(),
    };
    let target = Ingest::Udp {
        addr: ingest_addr.to_string(),
    };

    tokio::spawn(ingest::run(target, None, queue.clone(), stats.clone()));
    tokio::spawn(upstream::run_network(up, queue.clone(), stats.clone()));

    // Collect received lines (datagrams may batch several) until we have 3.
    let mut received: Vec<String> = Vec::new();
    let mut buf = vec![0u8; 65536];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while received.len() < 3 && tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(500), ingest_sock.recv(&mut buf)).await {
            Ok(Ok(n)) => {
                let text = std::str::from_utf8(&buf[..n]).unwrap();
                for line in text.split('\n').filter(|l| !l.trim().is_empty()) {
                    received.push(line.to_string());
                }
            }
            _ => continue,
        }
    }

    received.sort();
    let mut expected: Vec<String> = VALID.iter().map(|s| s.to_string()).collect();
    expected.sort();
    assert_eq!(
        received, expected,
        "exactly the 3 valid lines should arrive"
    );

    use std::sync::atomic::Ordering;
    assert_eq!(stats.invalid.load(Ordering::Relaxed), 1, "one invalid line");
    assert!(stats.forwarded.load(Ordering::Relaxed) >= 3);
}
