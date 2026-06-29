#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

pid_file="${SPREAD_MENUBAR_PID_FILE:-var/spreadfoundry_menubar.pid}"
log_file="${SPREAD_MENUBAR_LOG_FILE:-var/spreadfoundry_menubar.log}"
menubar_bin="${SPREAD_MENUBAR_BINARY:-apps/SpreadFoundryMenubar/.build/release/SpreadFoundryMenubar}"
launch_label="com.spreadfoundry.menubar"
launch_domain="gui/$(id -u)"
launch_script="var/spreadfoundry_menubar_launch.sh"
launch_plist="var/${launch_label}.plist"

is_running() {
  local pid="$1"
  [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
}

read_pid() {
  if [[ -f "$pid_file" ]]; then
    tr -d '[:space:]' < "$pid_file"
  fi
}

write_launch_files() {
  mkdir -p "$(dirname "$pid_file")" "$(dirname "$log_file")"
  cat > "$launch_script" <<EOF
#!/usr/bin/env bash
set -euo pipefail
cd "$repo_root"
echo "\$\$" > "$pid_file"
export SPREAD_ROOT="$repo_root"
exec "$repo_root/$menubar_bin"
EOF
  chmod +x "$launch_script"
  cat > "$launch_plist" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>$launch_label</string>
  <key>ProgramArguments</key>
  <array>
    <string>/bin/bash</string>
    <string>$repo_root/$launch_script</string>
  </array>
  <key>WorkingDirectory</key>
  <string>$repo_root</string>
  <key>RunAtLoad</key>
  <true/>
  <key>StandardOutPath</key>
  <string>$repo_root/$log_file</string>
  <key>StandardErrorPath</key>
  <string>$repo_root/$log_file</string>
</dict>
</plist>
EOF
}

start_menubar() {
  local pid
  pid="$(read_pid || true)"
  if is_running "$pid"; then
    echo "menubar already running pid=$pid"
    return 0
  fi
  rm -f "$pid_file"
  write_launch_files
  launchctl bootout "$launch_domain/$launch_label" >/dev/null 2>&1 || true
  launchctl bootstrap "$launch_domain" "$repo_root/$launch_plist"
  sleep 1
  pid="$(read_pid || true)"
  echo "started menubar launchd=$launch_label pid=${pid:-unknown} log=$log_file"
}

stop_menubar() {
  launchctl bootout "$launch_domain/$launch_label" >/dev/null 2>&1 || true
  local pid
  pid="$(read_pid || true)"
  if is_running "$pid"; then
    kill "$pid" 2>/dev/null || true
  fi
  rm -f "$pid_file"
  echo "menubar stopped"
}

status_menubar() {
  local pid
  pid="$(read_pid || true)"
  if is_running "$pid"; then
    echo "menubar running pid=$pid"
  else
    echo "menubar not running"
  fi
}

case "${1:-status}" in
  start)
    start_menubar
    ;;
  stop)
    stop_menubar
    ;;
  restart)
    stop_menubar
    start_menubar
    ;;
  status)
    status_menubar
    ;;
  log)
    mkdir -p "$(dirname "$log_file")"
    touch "$log_file"
    tail -n "${SPREAD_MENUBAR_LOG_LINES:-80}" "$log_file"
    ;;
  *)
    echo "usage: $0 {start|stop|restart|status|log}" >&2
    exit 2
    ;;
esac
