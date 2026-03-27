
Copy

#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# FanClash Live Poller — Render start script
#
# Render Background Workers need a long-running process.
# This script:
#   1. Installs cron on the container
#   2. Runs the poller once on startup (it self-schedules via cron)
#   3. Keeps cron daemon running so future self-scheduled runs fire
# ─────────────────────────────────────────────────────────────────────────────
 
set -e
 
echo "🚀 FanClash Live Poller starting on Render..."
 
# Install cron if not present (Render uses Debian/Ubuntu)
if ! command -v cron &> /dev/null; then
    echo "📦 Installing cron..."
    apt-get update -qq && apt-get install -y -qq cron
fi
 
# Run the poller once on startup
# It will self-schedule future runs via crontab then exit
echo "▶️  Running initial poller check..."
python /app/live_poller.py
 
# Start cron daemon in the foreground so the container stays alive
# and scheduled jobs fire on time
echo "⏰ Starting cron daemon..."
cron -f
 