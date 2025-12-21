//! Richmon-Babel Panel Indicator — the tower's vital signs visible from outside
//!
//! Subscribes to babel daemon events and sends indicator updates to the richmon
//! panel widget. Each active Claude session gets its own dot in the panel —
//! worker souls made visible through their current state.
//!
//! ## Usage
//!
//! ```bash
//! richmon-babel              # Run daemon (use systemctl)
//! ```
//!
//! ## Signal Flow
//!
//! ```text
//! babel events → subscriber → IndicatorEvent → richmon panel
//! ```
//!
//! ## State-Based Coloring
//!
//! - Idle: Dim gray (#666666)
//! - Thinking: Gold (#f0c040) - Claude generating response
//! - ToolUse: Cyan (#40c0f0) - Executing tool/command
//! - BackgroundTask: Teal (#40f0c0) - Background work in progress
//! - AwaitingInput: Rose (#f04080) - Needs user input
//! - Unknown: Darker gray (#454545)
//!
//! ## Protocol
//!
//! Events are sent as JSON lines over Unix datagram socket:
//! ```json
//! {"type":"Set","id":"k5","color":"#f0c040","workspace":4}
//! {"type":"Remove","id":"k5"}
//! ```

use anyhow::{Context, Result};
use std::os::unix::net::UnixDatagram;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

use claude_babel::events::BabelEvent;
use claude_babel::indicator::IndicatorEvent;
use claude_babel::logging::format_event;
use claude_babel::utility::ipc::{Request, Response};
use claude_babel::ActivityState;

const RICHMON_SOCKET: &str = "/tmp/richmon-post-babel.sock";

/// Map ActivityState to hex color
fn state_to_color(state: ActivityState) -> &'static str {
    match state {
        ActivityState::Idle => "#666666",           // Dim gray
        ActivityState::Thinking => "#f0c040",       // Gold
        ActivityState::ToolUse => "#40c0f0",        // Cyan
        ActivityState::PlanApproval => "#c080f0",   // Purple
        ActivityState::BackgroundTask => "#40f0c0", // Teal
        ActivityState::AwaitingInput => "#f04080",  // Rose
        ActivityState::Unknown => "#454545",        // Darker gray
    }
}

/// Format kitty window ID as indicator key
fn indicator_id(kitty_id: u64) -> String {
    format!("k{}", kitty_id)
}

/// Send indicator event to richmon panel
///
/// Fire-and-forget datagram - doesn't block if receiver isn't listening.
fn send_event(event: &IndicatorEvent) {
    let json = event.to_json();
    tracing::debug!("→ {}", json);

    if let Ok(socket) = UnixDatagram::unbound() {
        let _ = socket.send_to(json.as_bytes(), RICHMON_SOCKET);
    }
}

/// Fetch initial window list from babel and send Set events
async fn fetch_initial_state() -> Result<()> {
    use claude_babel::utility::ipc::send_request;

    tracing::info!("Fetching initial window list from babel");

    match send_request(&Request::List).await {
        Ok(Response::Windows { windows }) => {
            tracing::info!(count = windows.len(), "Got initial window list");
            for window in windows {
                let id = window.id();
                let state = claude_babel::utility::claude_discovery::get_window_activity_state(id);
                let event = IndicatorEvent::Set {
                    id: indicator_id(id),
                    color: state_to_color(state).to_string(),
                    workspace: window.workspace.unwrap_or(0) as u32,
                };
                send_event(&event);
            }
            Ok(())
        }
        Ok(other) => {
            tracing::warn!(?other, "Unexpected response to List request");
            Ok(())
        }
        Err(e) => {
            tracing::warn!(error = %e, "Failed to fetch initial window list");
            Ok(())
        }
    }
}

/// Subscribe to babel events
async fn subscribe_to_events() -> Result<()> {
    let socket_path = claude_babel::utility::ipc::socket_path();
    tracing::info!("Connecting to babel");

    let mut stream = UnixStream::connect(&socket_path)
        .await
        .context("Failed to connect to babel daemon")?;

    // Send subscribe request (empty events = all events)
    let request = serde_json::to_string(&Request::Subscribe { events: vec![] })?;
    stream.write_all(request.as_bytes()).await?;
    stream.write_all(b"\n").await?;

    let mut reader = BufReader::new(stream);
    let mut line = String::new();

    // Read subscription confirmation
    reader.read_line(&mut line).await?;
    let response: Response = serde_json::from_str(&line)?;
    line.clear();

    match response {
        Response::Subscribed { .. } => {
            tracing::info!("Subscribed to babel events");
        }
        Response::Error { message } => {
            anyhow::bail!("Subscription failed: {}", message);
        }
        _ => {
            anyhow::bail!("Unexpected response: {:?}", response);
        }
    }

    // Process events - pure event-driven, no polling
    loop {
        let bytes_read = reader.read_line(&mut line).await?;
        if bytes_read == 0 {
            tracing::info!("Babel connection closed");
            return Ok(());
        }

        let response: Response = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, "Failed to parse event, skipping");
                line.clear();
                continue;
            }
        };

        if let Response::Event { event } = response {
            if let Some(indicator_event) = handle_event(event.event) {
                send_event(&indicator_event);
            }
        }

        line.clear();
    }
}

/// Handle babel event - returns indicator event if state changed
fn handle_event(event: BabelEvent) -> Option<IndicatorEvent> {
    // Log event
    match &event {
        BabelEvent::ActivityPulse { .. } => {
            tracing::trace!("{}", format_event(&event));
        }
        BabelEvent::SessionStateChanged { .. } |
        BabelEvent::WindowWorkspaceChanged { .. } |
        BabelEvent::WSetSaved { .. } |
        BabelEvent::WSetLoaded { .. } |
        BabelEvent::DaemonShutdown => {
            tracing::info!("{}", format_event(&event));
        }
        _ => {
            tracing::debug!("{}", format_event(&event));
        }
    }

    // Convert to indicator event
    match event {
        BabelEvent::WindowAdded { kitty_id, workspace, .. } => {
            let state = claude_babel::utility::claude_discovery::get_window_activity_state(kitty_id);
            Some(IndicatorEvent::Set {
                id: indicator_id(kitty_id),
                color: state_to_color(state).to_string(),
                workspace: workspace.unwrap_or(0) as u32,
            })
        }

        BabelEvent::WindowRemoved { kitty_id } => {
            Some(IndicatorEvent::Remove {
                id: indicator_id(kitty_id),
            })
        }

        BabelEvent::SessionStateChanged { kitty_id, workspace, new_state, .. } => {
            Some(IndicatorEvent::Set {
                id: indicator_id(kitty_id),
                color: state_to_color(new_state).to_string(),
                workspace: workspace.unwrap_or(0) as u32,
            })
        }

        BabelEvent::WindowWorkspaceChanged { kitty_id, new_workspace, .. } => {
            // Need current state to send full Set event
            let state = claude_babel::utility::claude_discovery::get_window_activity_state(kitty_id);
            Some(IndicatorEvent::Set {
                id: indicator_id(kitty_id),
                color: state_to_color(state).to_string(),
                workspace: new_workspace.unwrap_or(0) as u32,
            })
        }

        // Events that don't affect indicators
        BabelEvent::ActivityPulse { .. } |
        BabelEvent::SessionMatched { .. } |
        BabelEvent::WSetSaved { .. } |
        BabelEvent::WSetLoaded { .. } |
        BabelEvent::DaemonShutdown => None,

        _ => None,
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    spaceship_std::init_logging!("richmon_babel", &spaceship_std::LoggingArgs::default());

    tracing::info!("Starting richmon-babel (event-driven indicators)");

    // Initial state fetch
    fetch_initial_state().await?;

    // Subscribe and process events
    subscribe_to_events().await?;

    Ok(())
}
