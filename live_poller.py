"""
FanClash Live Score Poller - Complete Edition
===============================================
1. Reads upcoming fixtures from MongoDB (READ ONLY)
2. Sleeps intelligently until game time
3. Polls Sofascore during live games
4. Forwards events to Rust backend (NO DB WRITES)
5. Triggers startup test notification via Rust

Rust backend handles:
- All database writes (games, timeline, stats)
- Push notifications (via FCM)
- WebSocket broadcasting
- Startup test notification delivery
"""

import time
import logging
import os
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timedelta, timezone
from typing import Optional, List

from curl_cffi import requests as cffi_requests
import requests as std_requests
from pymongo import MongoClient

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

DATABASE_URL = os.environ.get("DATABASE_URL", "mongodb+srv://engineercapiyo_db_user:CapiyoClash1999@cluster0.omepeze.mongodb.net/clashdb?retryWrites=true&w=majority&appName=Cluster0")
FANCLASH_API = os.environ.get("FANCLASH_API", "https://fanclash-api.onrender.com/api")
SOFASCORE_API = "https://api.sofascore.com/api/v1"
SOFASCORE_HOME = "https://www.sofascore.com"

NAIROBI_OFFSET = timedelta(hours=3)
POLL_INTERVAL_SEC = 10  # Poll every 10 seconds when game is live

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# HEALTH SERVER (for Render)
# ─────────────────────────────────────────────────────────────────────────────

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"FanClash Poller OK")

    def log_message(self, *args):
        pass

def start_health_server():
    port = int(os.environ.get("PORT", 8080))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logger.info(f"🌐 Health server listening on port {port}")

# ─────────────────────────────────────────────────────────────────────────────
# MONGODB - READ ONLY (Poller only reads fixtures, NO WRITES!)
# ─────────────────────────────────────────────────────────────────────────────

def connect_db():
    client = MongoClient(
        DATABASE_URL,
        serverSelectionTimeoutMS=15000,
        connectTimeoutMS=15000,
        socketTimeoutMS=45000,
    )
    client.admin.command("ping")
    games_col = client["clashdb"]["games"]
    logger.info("✅ Connected to MongoDB (READ ONLY)")
    return client, games_col

def get_kickoff_utc(fixture: dict) -> Optional[datetime]:
    try:
        date_str = fixture.get("date_iso", "")
        time_str = fixture.get("time", "00:00")
        if not date_str:
            return None
        naive = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
        return (naive - NAIROBI_OFFSET).replace(tzinfo=timezone.utc)
    except Exception as e:
        logger.warning(f"⚠️ Could not parse kick-off: {e}")
        return None

def get_upcoming_fixtures(games_col) -> list:
    fixtures = list(games_col.find(
        {"status": {"$in": ["upcoming", "live"]}},
        sort=[("date_iso", 1), ("time", 1)],
    ))
    result = []
    for f in fixtures:
        ko = get_kickoff_utc(f)
        if ko:
            f["_kickoff_utc"] = ko
            result.append(f)
    return result

def get_next_kickoff(fixtures: list) -> Optional[datetime]:
    now = datetime.now(timezone.utc)
    future = [f["_kickoff_utc"] for f in fixtures if f["_kickoff_utc"] > now]
    return min(future) if future else None

def is_game_live(fixture: dict) -> bool:
    now = datetime.now(timezone.utc)
    ko = fixture.get("_kickoff_utc")
    if not ko:
        return False
    mins_diff = (now - ko).total_seconds() / 60
    return -5 <= mins_diff <= 120

# ─────────────────────────────────────────────────────────────────────────────
# SOFASCORE API
# ─────────────────────────────────────────────────────────────────────────────

def make_session() -> cffi_requests.Session:
    session = cffi_requests.Session(impersonate="chrome124")
    session.headers.update({"Accept-Language": "en-US,en;q=0.9"})
    try:
        session.get(SOFASCORE_HOME, timeout=15)
    except Exception:
        pass
    session.headers.update({
        "Accept": "application/json, text/plain, */*",
        "Referer": f"{SOFASCORE_HOME}/",
        "Origin": SOFASCORE_HOME,
    })
    return session

def fetch_live_data(session: cffi_requests.Session, sofascore_id: int) -> Optional[dict]:
    try:
        time.sleep(0.5)
        resp = session.get(f"{SOFASCORE_API}/event/{sofascore_id}", timeout=15)
        if resp.status_code != 200:
            return None
        event = resp.json().get("event", {})
        
        return {
            "home_score": (event.get("homeScore") or {}).get("current", 0),
            "away_score": (event.get("awayScore") or {}).get("current", 0),
            "status_type": (event.get("status") or {}).get("type", ""),
            "status_code": (event.get("status") or {}).get("code", 0),
            "time_elapsed": event.get("time", {}).get("elapsed", 0),
            "incidents": event.get("incidents", []),
        }
    except Exception as e:
        logger.warning(f"⚠️ Error fetching event {sofascore_id}: {e}")
        return None

# ─────────────────────────────────────────────────────────────────────────────
# FORWARD TO RUST BACKEND (NO DB WRITES!)
# ─────────────────────────────────────────────────────────────────────────────

def forward_to_rust(fixture: dict, event_type: str, data: dict):
    """Forward event to Rust backend - Rust handles all DB writes"""
    try:
        payload = {
            "fixture_id": fixture["match_id"],
            "event_type": event_type,
            "home_score": data.get("home_score", fixture.get("home_score", 0)),
            "away_score": data.get("away_score", fixture.get("away_score", 0)),
            "minute": data.get("time_elapsed", 0),
            "scorer": data.get("scorer"),
            "player": data.get("player"),
            "team": data.get("team"),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        response = std_requests.post(
            f"{FANCLASH_API}/games/live-update",
            json=payload,
            timeout=5
        )
        
        if response.status_code == 200:
            logger.debug(f"✅ Forwarded {event_type} to Rust")
        else:
            logger.warning(f"⚠️ Rust returned {response.status_code}")
            
    except Exception as e:
        logger.error(f"❌ Failed to forward: {e}")

def send_startup_test_notification():
    """Send startup test notification via Rust backend"""
    logger.info("🔔 Sending startup test notification via Rust...")
    
    try:
        response = std_requests.post(
            f"{FANCLASH_API}/games/test-notification",
            json={
                "type": "startup_test",
                "message": "FanClash Live Poller is active!",
                "timestamp": datetime.now(timezone.utc).isoformat()
            },
            timeout=30
        )
        
        if response.status_code == 200:
            data = response.json()
            logger.info(f"✅ Test notification triggered! {data.get('message', '')}")
            return True
        else:
            logger.warning(f"⚠️ Rust returned {response.status_code}")
            return False
            
    except std_requests.exceptions.Timeout:  # ← Change from requests to std_requests
        logger.error("❌ Test notification timeout - Rust might be cold starting")
        return False
    except Exception as e:
        logger.error(f"❌ Failed to trigger test notification: {e}")
        return False

def fetch_and_forward_lineups(session, fixture):
    """Fetch lineups from Sofascore and forward to Rust"""
    sofascore_id = fixture.get("sofascore_id")
    if not sofascore_id:
        return
    
    try:
        resp = session.get(f"{SOFASCORE_API}/event/{sofascore_id}/lineups", timeout=10)
        if resp.status_code != 200:
            logger.warning(f"⚠️ Failed to fetch lineups: {resp.status_code}")
            return
        
        lineups_data = resp.json()
        
        response = std_requests.post(
            f"{FANCLASH_API}/lineups",
            json={
                "fixture_id": fixture["match_id"],
                "lineups": lineups_data,
                "timestamp": datetime.now(timezone.utc).isoformat()
            },
            timeout=5
        )
        
        if response.status_code == 200:
            logger.info(f"📋 Lineups forwarded for {fixture['home_team']} vs {fixture['away_team']}")
        else:
            logger.warning(f"⚠️ Rust returned {response.status_code} for lineups")
            
    except Exception as e:
        logger.error(f"❌ Failed to fetch/forward lineups: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# LIVE GAME POLLING
# ─────────────────────────────────────────────────────────────────────────────

def poll_live_game(session: cffi_requests.Session, fixture: dict):
    """Poll a single live game and forward events to Rust"""
    sofascore_id = fixture.get("sofascore_id")
    if not sofascore_id:
        logger.warning(f"⚠️ No sofascore_id for {fixture['home_team']} vs {fixture['away_team']}")
        return
    
    # Fetch lineups once when game starts
    if not fixture.get("lineups_fetched"):
        fetch_and_forward_lineups(session, fixture)
        fixture["lineups_fetched"] = True
    
    last_home = fixture.get("home_score", 0)
    last_away = fixture.get("away_score", 0)
    half_time_sent = False
    full_time_sent = False
    seen_incidents = set()
    
    logger.info(f"🔴 Starting live polling for {fixture['home_team']} vs {fixture['away_team']}")
    
    while True:
        live_data = fetch_live_data(session, sofascore_id)
        if not live_data:
            time.sleep(POLL_INTERVAL_SEC)
            continue
        
        home_score = live_data["home_score"]
        away_score = live_data["away_score"]
        time_elapsed = live_data["time_elapsed"]
        status_code = live_data["status_code"]
        status_type = live_data["status_type"]
        
        # Check for goal
        if home_score > last_home:
            logger.info(f"⚽ GOAL! {fixture['home_team']} scores! {home_score}-{away_score} ({time_elapsed}')")
            forward_to_rust(fixture, "goal", {
                **live_data,
                "scorer": "home_team",
                "player": None,
                "team": fixture["home_team"],
            })
            last_home = home_score
            
        elif away_score > last_away:
            logger.info(f"⚽ GOAL! {fixture['away_team']} scores! {home_score}-{away_score} ({time_elapsed}')")
            forward_to_rust(fixture, "goal", {
                **live_data,
                "scorer": "away_team",
                "player": None,
                "team": fixture["away_team"],
            })
            last_away = away_score
        
        # Forward score update
        if (home_score, away_score) != (last_home, last_away):
            forward_to_rust(fixture, "score", live_data)
        
        # Process incidents (yellow cards)
        for inc in live_data.get("incidents", []):
            inc_id = str(inc.get("id", ""))
            if inc_id in seen_incidents:
                continue
            seen_incidents.add(inc_id)
            
            inc_type = inc.get("incidentType", "").lower()
            inc_cls = inc.get("incidentClass", "").lower()
            is_home = inc.get("isHome", True)
            team = fixture["home_team"] if is_home else fixture["away_team"]
            minute = inc.get("time", {}).get("elapsed", time_elapsed)
            
            if inc_type == "card" and inc_cls == "yellow":
                player = inc.get("player", {}).get("name", "Unknown")
                logger.info(f"🟨 Yellow card - {team} ({player}) at {minute}'")
                forward_to_rust(fixture, "yellow_card", {
                    "time_elapsed": minute,
                    "player": player,
                    "team": team,
                })
        
        # Half time
        is_ht = (status_type == "pause" or status_code == 31)
        if is_ht and not half_time_sent:
            logger.info(f"⏸ Half-time: {fixture['home_team']} {home_score}-{away_score} {fixture['away_team']}")
            forward_to_rust(fixture, "half_time", live_data)
            half_time_sent = True
        
        # Full time
        if status_code in (100, 110, 120) and not full_time_sent:
            logger.info(f"🏁 Full-time: {fixture['home_team']} {home_score}-{away_score} {fixture['away_team']}")
            forward_to_rust(fixture, "full_time", live_data)
            full_time_sent = True
            break
        
        time.sleep(POLL_INTERVAL_SEC)
    
    logger.info(f"✅ Finished polling {fixture['home_team']} vs {fixture['away_team']}")

# ─────────────────────────────────────────────────────────────────────────────
# SMART SLEEP
# ─────────────────────────────────────────────────────────────────────────────

def smart_sleep(games_col):
    """Sleep until 1 hour before the next game"""
    all_fixtures = get_upcoming_fixtures(games_col)
    if not all_fixtures:
        logger.info("📭 No upcoming fixtures — sleeping 6 hours")
        time.sleep(21600)
        return

    next_ko = get_next_kickoff(all_fixtures)
    if not next_ko:
        logger.info("📭 No future kick-offs — sleeping 6 hours")
        time.sleep(21600)
        return

    now = datetime.now(timezone.utc)
    mins_to = (next_ko - now).total_seconds() / 60
    kickoff_eat = (next_ko + NAIROBI_OFFSET).strftime('%Y-%m-%d %H:%M')

    if mins_to > 60:
        sleep_mins = mins_to - 60
        logger.info(f"💤 Next game at {kickoff_eat} EAT — sleeping {sleep_mins:.0f} minutes")
        time.sleep(sleep_mins * 60)
    else:
        logger.info(f"⚽ Game at {kickoff_eat} EAT is starting soon — waking up")

# ─────────────────────────────────────────────────────────────────────────────
# MAIN LOOP
# ─────────────────────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 55)
    logger.info("⚽ FanClash Live Poller")
    logger.info("📡 Reads fixtures from MongoDB (READ ONLY)")
    logger.info("🔄 Polls Sofascore during live games")
    logger.info("📤 Forwards events to Rust backend")
    logger.info("💾 NO database writes (Rust handles that)")
    logger.info("=" * 55)

    # Start health server for Render
    start_health_server()
    
    # Connect to MongoDB (READ ONLY)
    mongo_client, games_col = connect_db()
    
    # Create Sofascore session
    session = make_session()
    
    # ========== SEND STARTUP TEST NOTIFICATION ==========
    logger.info("")
    logger.info("🔔 SENDING STARTUP TEST NOTIFICATION TO ALL USERS...")
    send_startup_test_notification()
    time.sleep(3)
    # ====================================================
    
    logger.info("🔄 Starting main polling loop...")
    
    try:
        while True:
            # Get all upcoming fixtures from MongoDB
            all_fixtures = get_upcoming_fixtures(games_col)
            
            # Check for live games
            live_fixtures = [f for f in all_fixtures if is_game_live(f)]
            
            if live_fixtures:
                logger.info(f"🔴 {len(live_fixtures)} live game(s) found")
                for fixture in live_fixtures:
                    poll_live_game(session, fixture)
            else:
                # No live games, sleep until next game
                smart_sleep(games_col)
                
    except KeyboardInterrupt:
        logger.info("\n⏹️ Stopped by user")
    finally:
        mongo_client.close()
        logger.info("🔌 MongoDB connection closed")

if __name__ == "__main__":
    main()