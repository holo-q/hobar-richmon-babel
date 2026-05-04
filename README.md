# xfce4-panel-richmon-babel

Thin paint-stream forwarder that relays
[babel](https://github.com/holo-q/babel)'s `PaintEvent::Window` payloads to
the [richmon](https://github.com/holo-q/xfce4-panel-richmon) XFCE panel
plugin over a Unix datagram socket. Babel owns all per-pane UX truth (color,
ring intensity, scale, outline, x position); this binary just ships bytes.

## Signal flow

```text
babel paint stream → SubscribePaint → PaintEvent::Window → richmon socket
```

Before babel's paint-stream refactor this was ~500 LOC of cached
`AgentKind`/`PlatformId` state, hex-color resolution, and geometry lookup.
All of that now lives in `babel::paint::resolve_color` and
`babel::daemon::BabelState` — the dot's color is decided in the daemon, this
binary only forwards.

## Build

This crate has **path dependencies** on two sibling crates that live in the
author's monorepo:

```
.
├── babel/                          ← github.com/holo-q/babel
├── spaceship-std/                  ← github.com/holo-q/spaceship-std (unpublished)
└── xfce4-panel-richmon-babel/      ← this crate
```

To build outside the monorepo, clone the siblings next to this one. Then:

```sh
cargo build --release
```

## Install

```sh
cargo install --path . --root ~/.local

# systemd user unit (template in repo)
install -m644 richmon-babel.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now richmon-babel.service
```

The unit declares `After=babel.service` so it starts after the babel daemon
is up. View logs:

```sh
journalctl --user -t richmon-babel -f
```

## License

GPL-2.0-or-later.
