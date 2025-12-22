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

/// Cache for mapping kitty_id → platform_window_id for geometry lookups
/// Populated from initial fetch and updated on WindowAdded events
type PlatformIdCache = std::collections::HashMap<u64, u64>;

/// Fetch initial window list from babel and send Set events
/// Windows are already sorted by screen position by babel
async fn fetch_initial_state() -> Result<PlatformIdCache> {
    use claude_babel::utility::ipc::send_request;
    use claude_babel::kitty::{get_pane, get_window_geometry};
    use std::collections::HashMap;

    tracing::info!("Fetching initial window list from babel");

    // Cache kitty_id -> platform_window_id for geometry lookups on events
    let mut platform_id_cache: PlatformIdCache = HashMap::new();

    match send_request(&Request::List).await {
        Ok(Response::Windows { windows }) => {
            tracing::info!(count = windows.len(), "Got initial window list");
            for window in &windows {
                let id = window.id();
                let platform_id = window.platform_window_id;

                // Cache the mapping for use in event handling
                platform_id_cache.insert(id, platform_id);

                // Get geometry: prefer patched kitty per-pane, fallback to xdotool
                let x_pos = get_pane(id)
                    .ok()
                    .flatten()
                    .and_then(|p| p.screen.map(|s| s.x))
                    .or_else(|| get_window_geometry(platform_id).ok().map(|g| g.x));

                let state = claude_babel::utility::claude_discovery::get_window_activity_state(id);
                let event = IndicatorEvent::Set {
                    id: indicator_id(id),
                    color: state_to_color(state).to_string(),
                    workspace: window.workspace.unwrap_or(0) as u32,
                    x_pos,
                    ring_intensity: 0.0,
                    has_outline: false,
                    scale: 1.0,
                };
                send_event(&event);
            }
            Ok(platform_id_cache)
        }
        Ok(other) => {
            tracing::warn!(?other, "Unexpected response to List request");
            Ok(HashMap::new())
        }
        Err(e) => {
            tracing::warn!(error = %e, "Failed to fetch initial window list");
            Ok(HashMap::new())
        }
    }
}

/// Subscribe to babel events
async fn subscribe_to_events(platform_id_cache: &mut PlatformIdCache) -> Result<()> {
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
            if let Some(indicator_event) = handle_event(event.event, platform_id_cache) {
                send_event(&indicator_event);
            }
        }

        line.clear();
    }
}

/// Get fresh geometry for a kitty pane
///
/// Strategy:
/// 1. Patched kitty's per-pane screen geometry (preferred - per-pane coords for splits)
/// 2. Fallback: xdotool via get_window_geometry (OS window level - temporary until patch deployed)
///
/// When patched kitty is running, #1 gives per-pane coordinates even for split layouts.
/// Until then, #2 gives OS window position (all panes in window share same x).
fn get_fresh_x_pos(kitty_id: u64, platform_id_cache: &mut PlatformIdCache) -> Option<i32> {
    use claude_babel::kitty::{get_pane, get_window_geometry};

    match get_pane(kitty_id) {
        Ok(Some(pane)) => {
            // Update platform_id cache
            platform_id_cache.insert(kitty_id, pane.platform_window_id);

            // Prefer patched kitty's per-pane screen geometry
            if let Some(screen) = &pane.screen {
                tracing::trace!(kitty_id, x = screen.x, "Kitty per-pane geometry");
                return Some(screen.x);
            }

            // Fallback: xdotool for OS window geometry (temporary until patched kitty deployed)
            match get_window_geometry(pane.platform_window_id) {
                Ok(geom) => {
                    tracing::trace!(kitty_id, x = geom.x, "xdotool OS window geometry");
                    Some(geom.x)
                }
                Err(e) => {
                    tracing::warn!(kitty_id, error = %e, "Geometry lookup failed");
                    None
                }
            }
        }
        Ok(None) => {
            tracing::debug!(kitty_id, "Pane not found in kitty");
            None
        }
        Err(e) => {
            tracing::warn!(kitty_id, error = %e, "Failed to query kitty");
            None
        }
    }
}

/// Handle babel event - returns indicator event if state changed
///
/// platform_id_cache: Maps kitty_id -> platform_window_id for geometry lookups.
/// Geometry is fetched fresh on position-affecting events (WindowAdded, WorkspaceChanged)
/// to ensure sorting reflects current screen layout.
fn handle_event(
    event: BabelEvent,
    platform_id_cache: &mut PlatformIdCache,
) -> Option<IndicatorEvent> {
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
            // Fresh geometry lookup for new window - essential for correct sorting
            let x_pos = get_fresh_x_pos(kitty_id, platform_id_cache);
            Some(IndicatorEvent::Set {
                id: indicator_id(kitty_id),
                color: state_to_color(state).to_string(),
                workspace: workspace.unwrap_or(0) as u32,
                x_pos,
                ring_intensity: 0.0,
                has_outline: false,
                scale: 1.0,
            })
        }

        BabelEvent::WindowRemoved { kitty_id } => {
            platform_id_cache.remove(&kitty_id);
            Some(IndicatorEvent::Remove {
                id: indicator_id(kitty_id),
            })
        }

        BabelEvent::SessionStateChanged { kitty_id, workspace, new_state, .. } => {
            // State change doesn't imply position change - use cached or fetch fresh
            // Fresh fetch ensures we catch any missed moves
            let x_pos = get_fresh_x_pos(kitty_id, platform_id_cache);
            Some(IndicatorEvent::Set {
                id: indicator_id(kitty_id),
                color: state_to_color(new_state).to_string(),
                workspace: workspace.unwrap_or(0) as u32,
                x_pos,
                ring_intensity: 0.0,
                has_outline: false,
                scale: 1.0,
            })
        }

        BabelEvent::WindowWorkspaceChanged { kitty_id, new_workspace, .. } => {
            // Workspace change often means position change (different monitor, etc.)
            // MUST refresh geometry for accurate sorting
            let state = claude_babel::utility::claude_discovery::get_window_activity_state(kitty_id);
            let x_pos = get_fresh_x_pos(kitty_id, platform_id_cache);
            Some(IndicatorEvent::Set {
                id: indicator_id(kitty_id),
                color: state_to_color(state).to_string(),
                workspace: new_workspace.unwrap_or(0) as u32,
                x_pos,
                ring_intensity: 0.0,
                has_outline: false,
                scale: 1.0,
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

    // Initial state fetch - returns platform_id cache for geometry lookups
    let mut platform_id_cache = fetch_initial_state().await?;

    // Subscribe and process events - geometry refreshed on each position-affecting event
    subscribe_to_events(&mut platform_id_cache).await?;

    Ok(())
}
