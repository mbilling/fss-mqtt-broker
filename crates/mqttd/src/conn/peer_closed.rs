//! Seeing a client hang up while the broker is not reading its socket (#825).
//!
//! A connection parked for ingress credit (ADR 0082 T3) stops reading so the
//! publisher backs up in its transport. A FIN queued behind unread publishes is
//! then invisible to the read loop: `read` would have to consume the publishes
//! first, and `peek` only sees the front of the queue. The kernel still knows:
//! epoll reports `EPOLLRDHUP` and kqueue `EV_EOF` as soon as the peer's FIN (or
//! RST) arrives, however much data is still buffered. [`PeerClosedWatch`]
//! registers a duplicate of the socket's descriptor for that readiness alone and
//! never reads from it, so backpressure is untouched. QUIC has no socket per
//! client; there the watch is the connection's own close (peer close or idle
//! timeout), which quinn tracks whether or not the stream is read.

use tokio::net::TcpStream;

/// Watches a client connection for the peer going away, without reading any
/// payload. For TCP it is built from the raw socket before any TLS or WebSocket
/// layer wraps it; [`PeerClosedWatch::new`] is `None` on non-unix targets.
pub struct PeerClosedWatch {
    source: Source,
    /// Readiness wakeups [`Self::closed`] has taken: the tests' proof it does not spin.
    #[cfg(test)]
    wakeups: std::sync::atomic::AtomicUsize,
}

enum Source {
    #[cfg(unix)]
    Tcp(tokio::io::unix::AsyncFd<std::os::fd::OwnedFd>),
    Quic(quinn::Connection),
}

impl std::fmt::Debug for PeerClosedWatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerClosedWatch").finish_non_exhaustive()
    }
}

impl PeerClosedWatch {
    fn from_source(source: Source) -> Self {
        Self {
            source,
            #[cfg(test)]
            wakeups: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Watch `stream`'s socket. `None` if the descriptor cannot be duplicated or
    /// registered, in which case the connection behaves as before #825: a paused
    /// connection sees the hangup once it resumes reading.
    #[must_use]
    #[cfg(unix)]
    pub fn new(stream: &TcpStream) -> Option<Self> {
        use std::os::fd::AsFd;
        // The duplicate shares the open file (and its O_NONBLOCK) with `stream`
        // but is a separate reactor registration, so it cannot steal readiness
        // from the read loop.
        let fd = stream.as_fd().try_clone_to_owned().ok()?;
        let fd = tokio::io::unix::AsyncFd::with_interest(fd, tokio::io::Interest::READABLE).ok()?;
        Some(Self::from_source(Source::Tcp(fd)))
    }

    /// Non-unix targets: no watch, the pre-#825 behaviour.
    #[must_use]
    #[cfg(not(unix))]
    pub fn new(_stream: &TcpStream) -> Option<Self> {
        None
    }

    /// Watch a QUIC client connection: resolves when it closes, by the client's
    /// `CONNECTION_CLOSE` or quinn's idle timeout.
    #[must_use]
    pub fn quic(conn: &quinn::Connection) -> Self {
        Self::from_source(Source::Quic(conn.clone()))
    }

    /// Resolves once the peer has gone: for TCP, closed its side (FIN) or reset
    /// the connection; for QUIC, the connection closed. Cancel-safe. TCP
    /// readiness is edge-triggered: plain "data arrived" wakeups are cleared and
    /// waited past, so a socket full of unread publishes does not spin.
    pub async fn closed(&self) {
        match &self.source {
            #[cfg(unix)]
            Source::Tcp(fd) => loop {
                match fd.readable().await {
                    Ok(mut guard) => {
                        #[cfg(test)]
                        self.wakeups
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let ready = guard.ready();
                        if ready.is_read_closed() || ready.is_error() {
                            return;
                        }
                        guard.clear_ready();
                    }
                    // The reactor is gone (runtime shutting down): nothing to watch.
                    Err(_) => std::future::pending::<()>().await,
                }
            },
            Source::Quic(conn) => {
                conn.closed().await;
            }
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::PeerClosedWatch;
    use std::sync::atomic::Ordering;
    use std::sync::{mpsc, Arc};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::oneshot;
    use tokio::time::timeout;

    /// Left unread in the server's receive buffer, written in [`CHUNKS`] pieces so
    /// the watch sees several "data arrived" wakeups.
    const UNREAD: usize = 64 * 1024;
    const CHUNKS: usize = 8;
    /// Wakeups the watch may take for the data: one per chunk, with slack. A
    /// spinning watch takes thousands in the idle window.
    const MAX_WAKEUPS: usize = 2 * CHUNKS;

    async fn pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (server, _) = listener.accept().await.unwrap();
        (client, server)
    }

    /// Watch `server` on a thread and runtime of its own, signalling once
    /// `closed()` resolves. A broken watch that spins then cannot starve the
    /// test's own timeouts: the test fails instead of hanging.
    fn watch_on_own_thread(server: &TcpStream) -> (Arc<PeerClosedWatch>, oneshot::Receiver<()>) {
        use std::os::fd::AsFd;
        let fd = server.as_fd().try_clone_to_owned().unwrap();
        let (watch_tx, watch_rx) = mpsc::channel();
        let (closed_tx, closed_rx) = oneshot::channel();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_io()
                .build()
                .unwrap();
            rt.block_on(async move {
                let socket = TcpStream::from_std(std::net::TcpStream::from(fd)).unwrap();
                let watch =
                    Arc::new(PeerClosedWatch::new(&socket).expect("a TCP socket can be watched"));
                watch_tx.send(watch.clone()).unwrap();
                watch.closed().await;
                let _ = closed_tx.send(());
            });
        });
        (watch_rx.recv().unwrap(), closed_rx)
    }

    #[tokio::test]
    async fn a_fin_behind_unread_data_is_seen_without_consuming_the_data_or_spinning() {
        let (mut client, mut server) = pair().await;
        let (watch, mut closed) = watch_on_own_thread(&server);
        for chunk in 0..CHUNKS {
            let before = watch.wakeups.load(Ordering::Relaxed);
            client.write_all(&[7u8; UNREAD / CHUNKS]).await.unwrap();
            // Each chunk wakes the watch, which must wait past it.
            timeout(Duration::from_secs(2), async {
                while watch.wakeups.load(Ordering::Relaxed) == before {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("chunk {chunk} never woke the watch"));
        }

        // Open with data waiting: not closed, and no further wakeups while idle.
        assert!(
            timeout(Duration::from_millis(300), &mut closed)
                .await
                .is_err(),
            "an open peer with unread data was reported closed"
        );
        let wakeups = watch.wakeups.load(Ordering::Relaxed);
        assert!(
            wakeups <= MAX_WAKEUPS,
            "the watch spun: {wakeups} wakeups for {CHUNKS} writes"
        );

        client.shutdown().await.unwrap();
        timeout(Duration::from_secs(2), closed)
            .await
            .expect("the FIN behind unread data was not seen")
            .unwrap();

        // The watch read nothing: every byte is still there for the read loop, then EOF.
        let mut all = Vec::new();
        server.read_to_end(&mut all).await.unwrap();
        assert_eq!(all.len(), UNREAD);
    }

    #[tokio::test]
    async fn a_reset_behind_unread_data_is_seen() {
        let (mut client, server) = pair().await;
        let (_watch, closed) = watch_on_own_thread(&server);
        client.write_all(&vec![7u8; UNREAD]).await.unwrap();
        // Linger 0: the close is an RST, not a FIN.
        client.set_zero_linger().unwrap();
        drop(client);
        timeout(Duration::from_secs(2), closed)
            .await
            .expect("the RST behind unread data was not seen")
            .unwrap();
    }
}
