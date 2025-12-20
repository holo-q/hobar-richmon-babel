# xfce4-panel-richmon-babel justfile

# Build release binary
build:
    cargo build --release

# Install binary and systemd service
install: build
    # Stop if running
    -systemctl --user stop richmon-babel.service 2>/dev/null
    # Install binary
    mkdir -p ~/bin
    cp target/release/richmon-babel ~/bin/
    # Install service
    mkdir -p ~/.config/systemd/user
    cp richmon-babel.service ~/.config/systemd/user/
    systemctl --user daemon-reload
    systemctl --user enable richmon-babel.service
    systemctl --user start richmon-babel.service
    @echo "✓ richmon-babel installed and started"

# Uninstall
uninstall:
    -systemctl --user stop richmon-babel.service 2>/dev/null
    -systemctl --user disable richmon-babel.service 2>/dev/null
    -rm ~/.config/systemd/user/richmon-babel.service
    -rm ~/bin/richmon-babel
    systemctl --user daemon-reload
    @echo "✓ richmon-babel uninstalled"

# View logs
logs:
    journalctl -t richmon_babel -f

# Check status
status:
    systemctl --user status richmon-babel.service

# Restart service
restart:
    systemctl --user restart richmon-babel.service
