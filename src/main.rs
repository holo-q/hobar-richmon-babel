//! Richmon-Babel Panel Indicator — the tower's vital signs visible from outside
//!
//! Subscribes to babel daemon events and sends manifest payloads to the richmon
//! panel indicator for multi-indicator display. Each active Claude session gets
//! its own dot in the panel — worker souls made visible, pulsing with their current labor.
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
//! babel events → subscriber → richmon manifest JSON → one dot per session
//! ```
//!
//! ## State-Based Coloring
//!
//! Unlike pulse-based coloring, this tracks actual session state from babel:
//! - Idle: Dim gray (#666666)
//! - Thinking: Gold (#f0c040) - Claude generating response
//! - ToolUse: Cyan (#40c0f0) - Executing tool/command
//! - BackgroundTask: Teal (#40f0c0) - Background work in progress
//! - AwaitingInput: Rose (#f04080) - Needs user input
//! - Unknown: Darker gray (#454545)
//!
//! ## Manifest Format
//!
//! ```json
//! {"session-id-1": {"color": "#f0c040", "workspace": 7}, ...}
//! ```
//!
//! Each entry is keyed by session ID (or kitty window ID fallback).
//! Color reflects current activity state from babel.

use anyhow::{Context, Result};
use indexmap::IndexMap;
use std::os::unix::net::UnixDatagram;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

use claude_babel::events::BabelEvent;
use claude_babel::logging::format_event;
use claude_babel::utility::ipc::{Request, Response};
use claude_babel::ActivityState;

const RICHMON_SOCKET: &str = "/tmp/richmon-post-babel.sock";

/// Map ActivityState to hex color — the hue of the worker's current breath
///
/// Colors match richspace-babel for consistency across panel indicators.
/// Each hue reveals what the worker is doing:
/// - Idle: Dim gray — resting
/// - Thinking: Gold — mind at work, generating thoughts
/// - ToolUse: Cyan — hands moving, executing commands
/// - PlanApproval: Purple — considering the path ahead
/// - BackgroundTask: Teal — working in the depths
/// - AwaitingInput: Rose — seeking the user's voice
/// - Unknown: Darker gray — newly arrived
fn state_to_color(state: ActivityState) -> &'static str {
    match state {
        ActivityState::Idle => "#666666",           // Dim gray
        ActivityState::Thinking => "#f0c040",       // Gold - generating
        ActivityState::ToolUse => "#40c0f0",        // Cyan - tool running
        ActivityState::PlanApproval => "#c080f0",   // Purple - plan review
        ActivityState::BackgroundTask => "#40f0c0", // Teal - bg work
        ActivityState::AwaitingInput => "#f04080",  // Rose - needs input
        ActivityState::Unknown => "#454545",        // Darker gray
    }
}

/// Tracked session state for manifest generation
#[derive(Debug)]
struct SessionState {
    /// Session ID if matched, None if only kitty_id known
    session_id: Option<String>,
    /// Current activity state from babel
    activity_state: ActivityState,
    /// XFCE workspace number (for grouping in panel display)
    workspace: Option<i32>,
}

impl SessionState {
    fn new(workspace: Option<i32>) -> Self {
        Self {
            session_id: None,
            activity_state: ActivityState::Unknown,
            workspace,
        }
    }

    /// Get color based on current activity state
    fn color(&self) -> &'static str {
        state_to_color(self.activity_state)
    }

    /// Get the key to use in manifest
    ///
    /// Always use kitty_id as the unique key - session_id can be shared across
    /// multiple windows (e.g., resuming a session in a new window), so it's not
    /// a reliable unique identifier for manifest entries.
    fn manifest_key(&self, kitty_id: u64) -> String {
        format!("k{}", kitty_id)
    }
}

/// Session tracker — gathering the workers into a unified view
///
/// Uses IndexMap to preserve left-to-right screen position order.
/// Babel sends windows in sorted order (by screen position), and we maintain
/// that ordering through all operations. This ensures the manifest JSON keys
/// appear in visual left-to-right order for the panel display, showing worker
/// souls as they appear across the screen.
struct SessionTracker {
    /// Active sessions keyed by kitty window ID (stable identifier)
    /// Order preserved: left-to-right screen position
    sessions: IndexMap<u64, SessionState>,
}

impl SessionTracker {
    fn new() -> Self {
        Self {
            sessions: IndexMap::new(),
        }
    }

    /// Add a new session (from WindowAdded)
    fn add(&mut self, kitty_id: u64, workspace: Option<i32>) {
        self.sessions.insert(kitty_id, SessionState::new(workspace));
    }

    /// Remove a session (from WindowRemoved)
    ///
    /// Uses shift_remove to maintain left-to-right order of remaining sessions.
    fn remove(&mut self, kitty_id: u64) {
        self.sessions.shift_remove(&kitty_id);
    }

    /// Update session ID (from SessionMatched)
    fn set_session_id(&mut self, kitty_id: u64, session_id: String) {
        if let Some(state) = self.sessions.get_mut(&kitty_id) {
            state.session_id = Some(session_id);
        }
    }

    /// Update activity state (from SessionStateChanged)
    ///
    /// This is the primary driver for color changes - state reflects
    /// what Claude is actually doing (thinking, tool use, awaiting input, etc.)
    fn update_state(&mut self, kitty_id: u64, new_state: ActivityState, workspace: Option<i32>) {
        if let Some(state) = self.sessions.get_mut(&kitty_id) {
            state.activity_state = new_state;
            if workspace.is_some() {
                state.workspace = workspace;
            }
        } else {
            // Session not tracked yet - add it with the state
            tracing::debug!("Tracker::AutoAdd {{ k{}, {:?} }}", kitty_id, new_state);
            let mut state = SessionState::new(workspace);
            state.activity_state = new_state;
            self.sessions.insert(kitty_id, state);
        }
    }

    /// Update workspace for a session (from window move events)
    fn update_workspace(&mut self, kitty_id: u64, workspace: Option<i32>) {
        if let Some(state) = self.sessions.get_mut(&kitty_id) {
            state.workspace = workspace;
        }
    }

    /// Generate JSON manifest of all tracked sessions — the tower's face made manifest
    ///
    /// Format: {"session-id-1": {"color": "#f0c040", "workspace": 7}, ...}
    /// Workspace included for grouping/spacing in panel display.
    ///
    /// Order preservation: IndexMap iteration maintains insertion order,
    /// which reflects left-to-right screen position (babel sends sorted windows).
    /// The manifest shows worker souls in their natural order.
    fn to_manifest(&self) -> String {
        let entries: Vec<String> = self.sessions.iter()
            .map(|(kitty_id, state)| {
                let key = state.manifest_key(*kitty_id);
                let color = state.color();
                let ws = state.workspace
                    .map(|w| format!(",\"workspace\":{}", w))
                    .unwrap_or_default();
                format!("\"{}\":{{\"color\":\"{}\"{}}}", key, color, ws)
            })
            .collect();

        format!("{{{}}}", entries.join(","))
    }

    /// Check if any sessions exist
    fn is_empty(&self) -> bool {
        self.sessions.is_empty()
    }
}

/// Post manifest to richmon panel
///
/// Uses sync UnixDatagram since we're in async context but this is fire-and-forget.
/// Falls back silently on error (richmon might not be running).
fn post_manifest(manifest: &str) {
    if manifest.is_empty() || manifest == "{}" {
        return; // Don't send empty manifests
    }

    tracing::debug!("Posting manifest: {}", manifest);

    // Fire-and-forget datagram - doesn't block if receiver isn't listening
    if let Ok(socket) = UnixDatagram::unbound() {
        let _ = socket.send_to(manifest.as_bytes(), RICHMON_SOCKET);
    }
}

/// Fetch initial window list from babel and populate tracker
async fn fetch_initial_state(tracker: &mut SessionTracker) -> Result<()> {
    use claude_babel::utility::ipc::send_request;

    tracing::info!("Fetching initial window list from babel");

    match send_request(&Request::List).await {
        Ok(Response::Windows { windows }) => {
            tracing::info!(count = windows.len(), "Got initial window list");
            for window in windows {
                let id = window.id();
                tracker.add(id, window.workspace);

                // Get current activity state from scrollback
                let state = claude_babel::utility::claude_discovery::get_window_activity_state(id);
                tracker.update_state(id, state, window.workspace);

                if let Some(session_id) = window.session_id {
                    tracker.set_session_id(id, session_id);
                }
            }
            // Post initial manifest
            if !tracker.is_empty() {
                post_manifest(&tracker.to_manifest());
            }
            Ok(())
        }
        Ok(other) => {
            tracing::warn!(?other, "Unexpected response to List request");
            Ok(()) // Not fatal - we'll pick up windows from events
        }
        Err(e) => {
            tracing::warn!(error = %e, "Failed to fetch initial window list");
            Ok(()) // Not fatal - we'll pick up windows from events
        }
    }
}

/// Subscribe to babel events and maintain session state
async fn run_subscriber() -> Result<()> {
    let socket_path = claude_babel::utility::ipc::socket_path();

    tracing::info!(socket = %socket_path.display(), "Connecting to babel");

    // Initialize session tracker and fetch initial state
    let mut tracker = SessionTracker::new();
    fetch_initial_state(&mut tracker).await?;

    let mut stream = UnixStream::connect(&socket_path)
        .await
        .with_context(|| format!("Failed to connect to babel at {}", socket_path.display()))?;

    // Send subscribe request (empty filter = all events)
    let request = Request::Subscribe { events: vec![] };
    let mut request_json = serde_json::to_string(&request)?;
    request_json.push('\n');
    stream.write_all(request_json.as_bytes()).await?;

    // Read subscription acknowledgment
    let mut reader = BufReader::new(&mut stream);
    let mut line = String::new();
    reader.read_line(&mut line).await?;

    let response: Response = serde_json::from_str(&line)
        .context("Failed to parse subscription response")?;

    match response {
        Response::Subscribed { subscriber_id } => {
            tracing::info!(subscriber_id, "Subscribed to babel events");
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
            let should_post = handle_event(&mut tracker, event.event);

            // Post manifest after state-changing events
            if should_post {
                post_manifest(&tracker.to_manifest());
            }
        }

        line.clear();
    }
}

/// Handle babel event - returns true if manifest should be posted
fn handle_event(tracker: &mut SessionTracker, event: BabelEvent) -> bool {
    // Log event with full context using standardized format
    match &event {
        // High-frequency events at trace level
        BabelEvent::ActivityPulse { .. } => {
            tracing::trace!("{}", format_event(&event));
        }
        // State changes at info level
        BabelEvent::SessionStateChanged { .. } |
        BabelEvent::WindowWorkspaceChanged { .. } |
        BabelEvent::WSetSaved { .. } |
        BabelEvent::WSetLoaded { .. } |
        BabelEvent::DaemonShutdown => {
            tracing::info!("{}", format_event(&event));
        }
        // Everything else at debug level
        _ => {
            tracing::debug!("{}", format_event(&event));
        }
    }

    // Process event and return whether to update manifest
    match event {
        // Window lifecycle - add/remove from tracking
        BabelEvent::WindowAdded { kitty_id, workspace, .. } => {
            tracker.add(kitty_id, workspace);
            // Get initial state for the new window
            let state = claude_babel::utility::claude_discovery::get_window_activity_state(kitty_id);
            tracker.update_state(kitty_id, state, workspace);
            true
        }

        BabelEvent::WindowRemoved { kitty_id } => {
            tracker.remove(kitty_id);
            true
        }

        // Session matching - associate session ID with kitty window
        BabelEvent::SessionMatched { kitty_id, session_id, .. } => {
            tracker.set_session_id(kitty_id, session_id);
            true
        }

        // State change - PRIMARY DRIVER for color updates
        BabelEvent::SessionStateChanged { kitty_id, workspace, new_state, .. } => {
            tracker.update_state(kitty_id, new_state, workspace);
            true
        }

        // Window moved to different workspace
        BabelEvent::WindowWorkspaceChanged { kitty_id, new_workspace, .. } => {
            tracker.update_workspace(kitty_id, new_workspace);
            true
        }

        // Activity pulses - we don't use these for coloring anymore
        BabelEvent::ActivityPulse { .. } => false,

        // WSet operations - just logged, no manifest update
        BabelEvent::WSetSaved { .. } |
        BabelEvent::WSetLoaded { .. } => false,

        // Daemon shutdown - clean exit
        BabelEvent::DaemonShutdown => false,

        // Other events - ignore
        _ => false,
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize centralized logging via spaceship-std
    // Uses init_logging! macro for automatic crate name detection
    // Journald sink: journalctl -t richmon_babel -f
    spaceship_std::init_logging!("richmon_babel", &spaceship_std::LoggingArgs::default());

    tracing::info!("Starting richmon-babel daemon (state-based coloring)");

    // Reconnect loop - babel might restart
    loop {
        match run_subscriber().await {
            Ok(()) => {
                tracing::info!("Subscriber exited cleanly, reconnecting...");
            }
            Err(e) => {
                tracing::warn!(error = %e, "Subscriber error, reconnecting...");
            }
        }

        // Wait before reconnecting
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    }
}
