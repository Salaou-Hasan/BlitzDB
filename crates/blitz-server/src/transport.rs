//! Async TCP transport for BlitzDB.
//!
//! Each connection runs the same loop: read bytes, pop complete frames
//! with [`FrameCodec`], dispatch the [`Request`] against the shared
//! [`BlitzServer`], write back the [`Response`]. The server is held as an
//! `Arc` and every engine operation takes `&self`, so connections run
//! fully concurrently; table-level engine locks provide the parallelism.
//!
//! Payloads are binary end to end: [`blitz_types::Value`] is encoded with
//! the protocol's tag-length scheme, so reads never touch JSON.

use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use blitz_core::TableEngine;
use blitz_protocol::{BatchResponse, FrameCodec, Incoming, Op, Request, Response, RowView};
use blitz_types::id::RowId;
use blitz_types::row::Row;

use crate::server::BlitzServer;

/// Releases a connection slot when dropped, so early returns and errors
/// cannot leak admission.
struct ConnectionGuard {
    server: Option<Arc<BlitzServer>>,
}

impl ConnectionGuard {
    fn new(server: &Arc<BlitzServer>) -> Option<Self> {
        if server.try_acquire_connection() {
            Some(Self {
                server: Some(Arc::clone(server)),
            })
        } else {
            None
        }
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        if let Some(server) = self.server.take() {
            server.release_connection();
        }
    }
}

fn row_to_view(id: RowId, row: &Row) -> RowView {
    RowView {
        id: id.as_u64(),
        values: row.values.clone(),
    }
}

/// Execute one request. Infallible by design: engine failures become
/// `Response::err` payloads instead of dropped connections.
fn dispatch(server: &BlitzServer, req: Request) -> Response {
    let id = req.id;
    match req.op {
        Op::Ping => Response::ok(id, Vec::new()),
        Op::Insert => {
            let values = match req.values {
                Some(v) => v,
                None => return Response::err(id, "insert requires values"),
            };
            let mut row = Row::new(RowId::new(0));
            for (k, v) in &values {
                row.set(k.clone(), v.clone());
            }
            match server.engine().insert(&req.table, row) {
                Ok(assigned) => Response::ok(
                    id,
                    vec![RowView {
                        id: assigned.as_u64(),
                        values,
                    }],
                ),
                Err(e) => Response::err(id, e.to_string()),
            }
        }
        Op::Get => {
            let row_id = match req.row_id {
                Some(rid) => RowId::new(rid),
                None => return Response::err(id, "get requires row_id"),
            };
            // Zero-copy read: serialize straight off the shared handle.
            match server.engine().get_arc(&req.table, row_id) {
                Ok(Some(row)) => Response::ok(id, vec![row_to_view(row_id, &row)]),
                Ok(None) => Response::err(id, format!("row not found: {}", row_id)),
                Err(e) => Response::err(id, e.to_string()),
            }
        }
        Op::Update => {
            let row_id = match req.row_id {
                Some(rid) => RowId::new(rid),
                None => return Response::err(id, "update requires row_id"),
            };
            let values = match req.values {
                Some(v) => v,
                None => return Response::err(id, "update requires values"),
            };
            match server.engine().update(&req.table, row_id, values) {
                Ok(row) => Response::ok(id, vec![row_to_view(row_id, &row)]),
                Err(e) => Response::err(id, e.to_string()),
            }
        }
        Op::Delete => {
            let row_id = match req.row_id {
                Some(rid) => RowId::new(rid),
                None => return Response::err(id, "delete requires row_id"),
            };
            match server.engine().delete(&req.table, row_id) {
                Ok(true) => Response::ok(id, Vec::new()),
                Ok(false) => Response::err(id, format!("row not found: {}", row_id)),
                Err(e) => Response::err(id, e.to_string()),
            }
        }
        Op::Scan => match server.engine().scan_arcs(&req.table) {
            // Zero-copy scan: one Arc bump per row, no deep clones.
            Ok(rows) => {
                let views = rows.iter().map(|row| row_to_view(row.id, row)).collect();
                Response::ok(id, views)
            }
            Err(e) => Response::err(id, e.to_string()),
        },
    }
}

/// Serve one connection until the client disconnects or a fatal I/O or
/// framing error occurs.
async fn handle_connection(server: Arc<BlitzServer>, mut socket: TcpStream) -> Result<()> {
    let _guard = match ConnectionGuard::new(&server) {
        Some(g) => g,
        None => return Ok(()), // at capacity: close immediately
    };

    // Disable Nagle: this is a request/response protocol with small frames,
    // so waiting to coalesce segments would add pure latency.
    socket.set_nodelay(true).context("failed to set TCP_NODELAY")?;

    let codec = FrameCodec::new(server.config().max_message_size);
    let mut staging = BytesMut::new();

    loop {
        let n = socket
            .read_buf(&mut staging)
            .await
            .context("failed to read from socket")?;
        if n == 0 {
            return Ok(()); // orderly shutdown
        }
        while let Some(frame) = codec
            .feed(&mut staging)
            .context("framing error: closing connection")?
        {
            let incoming = codec
                .decode_incoming(frame)
                .context("decode error: closing connection")?;
            // Batch and single share one frame budget: a batch of N costs
            // one read + one write instead of N round trips.
            let encoded = match incoming {
                Incoming::Single(req) => {
                    let resp = dispatch(&server, req);
                    codec.encode_response(&resp).context("encode error")?
                }
                Incoming::Batch(batch) => {
                    let mut results = Vec::with_capacity(batch.ops.len());
                    for op in batch.ops {
                        results.push(dispatch(&server, op));
                    }
                    let bresp = BatchResponse {
                        id: batch.id,
                        results,
                    };
                    codec
                        .encode_batch_response(&bresp)
                        .context("encode error")?
                }
            };
            socket
                .write_all(&encoded)
                .await
                .context("failed to write to socket")?;
        }
    }
}

/// Accept connections forever, spawning one task per connection.
pub async fn serve(server: Arc<BlitzServer>, listener: TcpListener) -> Result<()> {
    loop {
        let (socket, _peer) = listener.accept().await.context("accept failed")?;
        let server = Arc::clone(&server);
        tokio::spawn(async move {
            if let Err(e) = handle_connection(server, socket).await {
                tracing::debug!("connection ended: {:#}", e);
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use blitz_types::value::Value;
    use std::collections::HashMap;
    use tokio::net::TcpStream;

    struct Client {
        read: tokio::net::tcp::OwnedReadHalf,
        write: tokio::net::tcp::OwnedWriteHalf,
        buf: BytesMut,
        codec: FrameCodec,
    }

    impl Client {
        async fn connect(addr: std::net::SocketAddr) -> Result<Self> {
            let stream = TcpStream::connect(addr).await?;
            let (read, write) = stream.into_split();
            Ok(Self {
                read,
                write,
                buf: BytesMut::new(),
                codec: FrameCodec::with_default_limit(),
            })
        }

        async fn roundtrip(&mut self, req: &Request) -> Result<Response> {
            let frame = self.codec.encode_request(req)?;
            self.write.write_all(&frame).await?;
            loop {
                if let Some(payload) = self.codec.feed(&mut self.buf)? {
                    return Ok(self.codec.decode_response(payload)?);
                }
                let n = self.read.read_buf(&mut self.buf).await?;
                assert!(n > 0, "server closed connection unexpectedly");
            }
        }
    }

    fn values(pairs: &[(&str, Value)]) -> HashMap<String, Value> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    async fn roundtrip_batch(
        client: &mut Client,
        id: u64,
        ops: Vec<Request>,
    ) -> Result<BatchResponse> {
        use blitz_protocol::BatchRequest;
        let frame = client
            .codec
            .encode_batch_request(&BatchRequest { id, ops })?;
        client.write.write_all(&frame).await?;
        loop {
            if let Some(payload) = client.codec.feed(&mut client.buf)? {
                return Ok(client.codec.decode_batch_response(payload)?);
            }
            let n = client.read.read_buf(&mut client.buf).await?;
            assert!(n > 0, "server closed connection unexpectedly");
        }
    }

    #[tokio::test]
    async fn test_e2e_ping_insert_get_scan_delete() {
        let server = Arc::new(BlitzServer::new());
        server.start().await.unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(Arc::clone(&server), listener));

        let mut client = Client::connect(addr).await.unwrap();

        // Ping.
        let resp = client.roundtrip(&Request::ping(1)).await.unwrap();
        assert_eq!(resp.id, 1);
        assert!(resp.ok);

        // Insert.
        let resp = client
            .roundtrip(&Request {
                id: 2,
                op: Op::Insert,
                table: "users".into(),
                row_id: None,
                values: Some(values(&[
                    ("id", Value::Int64(1)),
                    ("name", Value::String("Alice".into())),
                    ("email", Value::String("alice@example.com".into())),
                ])),
            })
            .await
            .unwrap();
        assert!(resp.ok, "insert failed: {:?}", resp.error);
        let assigned = resp.rows[0].id;

        // Get.
        let resp = client
            .roundtrip(&Request {
                id: 3,
                op: Op::Get,
                table: "users".into(),
                row_id: Some(assigned),
                values: None,
            })
            .await
            .unwrap();
        assert!(resp.ok);
        assert_eq!(
            resp.rows[0].values.get("name"),
            Some(&Value::String("Alice".into()))
        );

        // Update.
        let resp = client
            .roundtrip(&Request {
                id: 4,
                op: Op::Update,
                table: "users".into(),
                row_id: Some(assigned),
                values: Some(values(&[("name", Value::String("Bob".into()))])),
            })
            .await
            .unwrap();
        assert!(resp.ok, "update failed: {:?}", resp.error);
        assert_eq!(
            resp.rows[0].values.get("name"),
            Some(&Value::String("Bob".into()))
        );

        // Scan.
        let resp = client
            .roundtrip(&Request {
                id: 5,
                op: Op::Scan,
                table: "users".into(),
                row_id: None,
                values: None,
            })
            .await
            .unwrap();
        assert!(resp.ok);
        assert_eq!(resp.rows.len(), 1);

        // Delete.
        let resp = client
            .roundtrip(&Request {
                id: 6,
                op: Op::Delete,
                table: "users".into(),
                row_id: Some(assigned),
                values: None,
            })
            .await
            .unwrap();
        assert!(resp.ok);

        // Get after delete -> error response, connection stays alive.
        let resp = client
            .roundtrip(&Request {
                id: 7,
                op: Op::Get,
                table: "users".into(),
                row_id: Some(assigned),
                values: None,
            })
            .await
            .unwrap();
        assert!(!resp.ok);
        assert!(resp.error.is_some());

        // Unknown table -> error response, connection stays alive.
        let resp = client
            .roundtrip(&Request {
                id: 8,
                op: Op::Scan,
                table: "nope".into(),
                row_id: None,
                values: None,
            })
            .await
            .unwrap();
        assert!(!resp.ok);
    }

    #[tokio::test]
    async fn test_e2e_batch_mixed_results() {
        let server = Arc::new(BlitzServer::new());
        server.start().await.unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(Arc::clone(&server), listener));

        let mut client = Client::connect(addr).await.unwrap();

        // One frame, five ops: insert, get, update, get-missing (err), scan.
        let bresp = roundtrip_batch(
            &mut client,
            50,
            vec![
                Request {
                    id: 51,
                    op: Op::Insert,
                    table: "users".into(),
                    row_id: None,
                    values: Some(values(&[
                        ("id", Value::Int64(9)),
                        ("name", Value::String("Batch".into())),
                        ("email", Value::String("batch@example.com".into())),
                    ])),
                },
                Request {
                    id: 52,
                    op: Op::Get,
                    table: "users".into(),
                    row_id: Some(1),
                    values: None,
                },
                Request {
                    id: 53,
                    op: Op::Update,
                    table: "users".into(),
                    row_id: Some(1),
                    values: Some(values(&[("name", Value::String("Batched".into()))])),
                },
                Request {
                    id: 54,
                    op: Op::Get,
                    table: "users".into(),
                    row_id: Some(9999),
                    values: None,
                },
                Request {
                    id: 55,
                    op: Op::Scan,
                    table: "users".into(),
                    row_id: None,
                    values: None,
                },
            ],
        )
        .await
        .unwrap();

        assert_eq!(bresp.id, 50);
        assert_eq!(bresp.results.len(), 5);
        assert!(bresp.results[0].ok); // insert
        assert!(bresp.results[1].ok); // get
        assert_eq!(
            bresp.results[2].rows[0].values.get("name"),
            Some(&Value::String("Batched".into()))
        );
        // Partial failure is per-op: missing row errs, frame still succeeds.
        assert!(!bresp.results[3].ok);
        assert_eq!(bresp.results[4].rows.len(), 1); // scan sees the insert
    }

    #[tokio::test]
    async fn test_many_concurrent_clients() {
        let server = Arc::new(BlitzServer::new());
        server.start().await.unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(Arc::clone(&server), listener));

        let mut handles = Vec::new();
        for c in 0..16 {
            handles.push(tokio::spawn(async move {
                let mut client = Client::connect(addr).await.unwrap();
                for i in 0..25 {
                    let resp = client
                        .roundtrip(&Request {
                            id: (c * 100 + i) as u64,
                            op: Op::Insert,
                            table: "users".into(),
                            row_id: None,
                            values: Some(values(&[
                                ("id", Value::Int64(c * 100 + i)),
                                ("name", Value::String("n".into())),
                                (
                                    "email",
                                    Value::String(format!("c{c}i{i}@x.com").into()),
                                ),
                            ])),
                        })
                        .await
                        .unwrap();
                    assert!(resp.ok, "insert failed: {:?}", resp.error);
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        assert_eq!(server.engine().count("users").unwrap(), 16 * 25);
        // Disconnects are observed asynchronously; wait for slots to drain.
        for _ in 0..200 {
            if server.connection_count() == 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(server.connection_count(), 0);
    }

    #[tokio::test]
    async fn test_admission_control_rejects_over_capacity() {
        use crate::server::ServerConfig;

        let mut cfg = ServerConfig::default();
        cfg.max_connections = 2;
        let server = Arc::new(BlitzServer::with_config(cfg));
        server.start().await.unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(serve(Arc::clone(&server), listener));

        let _a = TcpStream::connect(addr).await.unwrap();
        let _b = TcpStream::connect(addr).await.unwrap();
        // Wait until both slots are admitted.
        for _ in 0..200 {
            if server.connection_count() == 2 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(server.connection_count(), 2);

        // Third connection must be closed immediately (EOF on read).
        let mut c = TcpStream::connect(addr).await.unwrap();
        let mut one = [0u8; 1];
        let n = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            c.read(&mut one),
        )
        .await
        .expect("server did not close excess connection")
        .unwrap();
        assert_eq!(n, 0);
        assert_eq!(server.connection_count(), 2);
    }
}
