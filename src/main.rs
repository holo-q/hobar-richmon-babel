//! Richmon-Babel — thin paint-stream forwarder
//!
//! Babel is authoritative over per-pane UX (color, ring intensity, scale,
//! outline, x_pos). This puppet just relays `PaintEvent::Window` payloads
//! from babel's paint stream to the richmon panel widget over its
//! datagram socket.
//!
//! Before the paint-stream refactor this binary was ~500 LOC of cached
//! AgentKind/PlatformId state, hex-color resolution, and geometry lookup.
//! All of that lives in `babel::paint::resolve_color` and
//! `babel::daemon::BabelState` now — the dot's color is decided in
//! the daemon, this binary only ships bytes.
//!
//! ## Signal Flow
//!
//! ```text
//! babel paint stream → SubscribePaint → PaintEvent::Window → richmon socket
//! ```

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::os::unix::net::UnixDatagram;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::sync::Mutex;

use babel::indicator::IndicatorEvent;
use babel::paint::PaintEvent;
use babel::utility::ipc::{Request, Response};

const RICHMON_SOCKET: &str = "/tmp/richmon-post-babel.sock";

/// Send an IndicatorEvent to richmon's panel widget. Fire-and-forget —
/// richmon is a passive UI; if its socket isn't bound (e.g. during panel
/// restart), we drop the datagram and let the replay loop catch it.
fn send_event(event: &IndicatorEvent) {
    let json = event.to_json();
    tracing::debug!("→ {}", json);
    if let Ok(socket) = UnixDatagram::unbound() {
        let _ = socket.send_to(json.as_bytes(), RICHMON_SOCKET);
    }
}

/// Cache of currently-active indicators, keyed by indicator id ("k42").
///
/// Babel guarantees idempotency on its end (a fresh subscription replays
/// full state via the paint stream's initial Reset+Set burst). We mirror
/// that locally so a richmon panel restart between paint events still gets
/// the current image — the replay loop walks this map every 2s.
type ActiveIndicators = Arc<Mutex<HashMap<String, IndicatorEvent>>>;

fn remember_indicator_event(
    active_indicators: &mut HashMap<String, IndicatorEvent>,
    event: &IndicatorEvent,
) {
    match event {
        IndicatorEvent::Set { id, .. } => {
            active_indicators.insert(id.clone(), event.clone());
        }
        IndicatorEvent::Remove { id } => {
            active_indicators.remove(id);
        }
        IndicatorEvent::Clear => {
            active_indicators.clear();
        }
    }
}

/// Periodically replay the active indicator image so panel/socket
/// restarts converge without waiting for the next babel paint event.
async fn replay_active_indicators(active_indicators: ActiveIndicators) {
    loop {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let events: Vec<IndicatorEvent> = {
            let active = active_indicators.lock().await;
            active.values().cloned().collect()
        };
        if events.is_empty() {
            continue;
        }
        tracing::debug!(count = events.len(), "Replaying active richmon indicators");
        for event in events {
            send_event(&event);
        }
    }
}

/// Subscribe to babel's paint stream and forward Window paint events to richmon.
///
/// Workspace paint events are observed but not forwarded — richspace-babel
/// owns that strand. The paint stream replays full state on connect, so
/// the initial connect handshake is also our initial-state fetch.
async fn subscribe_to_paint(active_indicators: ActiveIndicators) -> Result<()> {
    let socket_path = babel::utility::ipc::socket_path();
    tracing::info!("Connecting to babel paint stream");

    let mut stream = UnixStream::connect(&socket_path)
        .await
        .context("Failed to connect to babel daemon")?;

    let request = serde_json::to_string(&Request::SubscribePaint)?;
    stream.write_all(request.as_bytes()).await?;
    stream.write_all(b"\n").await?;

    let mut reader = BufReader::new(stream);
    let mut line = String::new();

    // Subscription ack
    reader.read_line(&mut line).await?;
    let response: Response = serde_json::from_str(&line)?;
    line.clear();
    match response {
        Response::Subscribed { subscriber_id } => {
            tracing::info!(subscriber_id, "Subscribed to babel paint stream");
        }
        Response::Error { message } => anyhow::bail!("Subscription failed: {}", message),
        _ => anyhow::bail!("Unexpected response: {:?}", response),
    }

    // Pure forwarding loop. No state, no policy — babel decided everything.
    loop {
        let bytes_read = reader.read_line(&mut line).await?;
        if bytes_read == 0 {
            tracing::info!("Babel paint stream closed");
            return Ok(());
        }

        let response: Response = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, line = line.trim(), "Failed to parse paint event");
                line.clear();
                continue;
            }
        };

        match response {
            Response::PaintEvent {
                event: PaintEvent::Window(indicator_event),
            } => {
                send_event(&indicator_event);
                let mut active = active_indicators.lock().await;
                remember_indicator_event(&mut active, &indicator_event);
            }
            Response::PaintEvent {
                event: PaintEvent::Reset,
            } => {
                // Daemon restart or full state replay incoming — drop our
                // cached image so the upcoming Set burst becomes the new
                // truth and stale dots don't linger.
                tracing::info!("Paint reset — clearing local indicator cache");
                let mut active = active_indicators.lock().await;
                active.clear();
                // Push a Clear to richmon so the panel widget also wipes.
                send_event(&IndicatorEvent::Clear);
            }
            Response::PaintEvent {
                event: PaintEvent::Workspace(_),
            } => {
                // Not richmon's concern — richspace-babel handles workspace paint.
            }
            other => {
                tracing::trace!(?other, "Non-paint response on paint stream — ignoring");
            }
        }

        line.clear();
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    spaceship_std::init_logging!("richmon_babel", &spaceship_std::LoggingArgs::default());
    tracing::info!("Starting richmon-babel (paint-stream forwarder)");

    let active_indicators: ActiveIndicators = Arc::new(Mutex::new(HashMap::new()));
    tokio::spawn(replay_active_indicators(active_indicators.clone()));

    // Reconnect loop — babel may restart, paint stream may close. The
    // SubscribePaint replay protocol means we don't lose any state across
    // a reconnect; the daemon resends the full image on each subscribe.
    loop {
        if let Err(e) = subscribe_to_paint(active_indicators.clone()).await {
            tracing::warn!(error = %e, "Paint subscription error, reconnecting in 3s");
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}
