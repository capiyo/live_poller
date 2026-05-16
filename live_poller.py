"""
FanClash Live Score Poller - WORKING VERSION
"""

import time
import logging
import os
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
import json
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any

from curl_cffi import requests as cffi_requests
import requests as std_requests

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

FANCLASH_API = os.environ.get("FANCLASH_API", "https://fanclash-api.onrender.com/api")
SOFASCORE_API = "https://api.sofascore.com/api/v1"
SOFASCORE_HOME = "https://www.sofascore.com"

NAIROBI_OFFSET = timedelta(hours=3)
POLL_INTERVAL_SEC = 10

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# HEALTH SERVER
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
# RUST BACKEND API
# ─────────────────────────────────────────────────────────────────────────────

def get_fixtures_from_rust() -> List[Dict[str, Any]]:
    try:
        resp = std_requests.get(f"{FANCLASH_API}/games/upcoming", timeout=10)
        if resp.status_code == 200:
            fixtures = resp.json()
            result = []
            for f in fixtures:
                date_iso = f.get("date_iso", "")
                time_str = f.get("time", "00:00")
                kickoff_utc = None
                try:
                    # Parse as EAT, convert to UTC
                    naive_eat = datetime.strptime(f"{date_iso} {time_str}", "%Y-%m-%d %H:%M")
                    kickoff_utc = (naive_eat - NAIROBI_OFFSET).replace(tzinfo=timezone.utc)
                except Exception as e:
                    logger.warning(f"Could not parse kickoff: {e}")
                
                result.append({
                    "match_id": f.get("match_id"),
                    "sofascore_id": f.get("sofascore_id"),
                    "home_team": f.get("home_team"),
                    "away_team": f.get("away_team"),
                    "home_score": f.get("home_score", 0),
                    "away_score": f.get("away_score", 0),
                    "status": f.get("status", "upcoming"),
                    "_kickoff_utc": kickoff_utc,
                    "lineups_fetched": False,
                })
            return result
        return []
    except Exception as e:
        logger.error(f"Failed to fetch fixtures: {e}")
        return []

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
        logger.warning(f"Error fetching event: {e}")
        return None

def auto_fetch_lineups(session: cffi_requests.Session, fixture: Dict[str, Any]) -> bool:
    """Automatically fetch lineups from Sofascore and forward to Rust"""
    sofascore_id = fixture.get("sofascore_id")
    if not sofascore_id or fixture.get("lineups_fetched"):
        return False
    
    try:
        resp = session.get(f"{SOFASCORE_API}/event/{sofascore_id}/lineups", timeout=10)
        if resp.status_code == 200:
            lineups_data = resp.json()
            
            # Debug: Log the structure to understand where names are
            if lineups_data.get("home", {}).get("players"):
                first_player = lineups_data["home"]["players"][0]
                logger.info(f"🔍 Sofascore player data structure: {json.dumps(first_player, indent=2)[:500]}")
            
            # Helper function to extract player name from various possible fields
            def get_player_name(player):
                # Try all possible field names that might contain the name
                name_fields = ["name", "fullName", "displayName", "playerName", "shortName"]
                for field in name_fields:
                    if field in player and player[field]:
                        return str(player[field])
                
                # If no name found, try to get from player object
                if "player" in player:
                    player_obj = player["player"]
                    for field in name_fields:
                        if field in player_obj and player_obj[field]:
                            return str(player_obj[field])
                
                # Last resort - use jersey number as identifier
                jersey = player.get("jerseyNumber", "Unknown")
                return f"Player #{jersey}"
            
            # Helper function to safely get player data
            def safe_player_data(player):
                # Extract name dynamically
                player_name = get_player_name(player)
                
                # Handle jersey_number
                jersey = player.get("jerseyNumber")
                if jersey is None:
                    jersey = 0
                elif isinstance(jersey, str):
                    try:
                        jersey = int(jersey) if jersey.isdigit() else 0
                    except:
                        jersey = 0
                elif not isinstance(jersey, int):
                    jersey = 0
                
                # Handle position
                position = player.get("position") or "Unknown"
                
                # Get player ID
                player_id = player.get("playerId") or player.get("id")
                if not player_id and "player" in player:
                    player_id = player["player"].get("id")
                
                return {
                    "name": player_name,
                    "position": str(position),
                    "jerseyNumber": jersey,
                    "captain": bool(player.get("captain", False)),
                    "lineup": bool(player.get("lineup", True)),
                    "playerId": str(player_id) if player_id else None
                }
            
            # Process home team
            home_players = []
            home_bench = []
            
            for player in lineups_data.get("home", {}).get("players", []):
                player_data = safe_player_data(player)
                if player.get("lineup", True):
                    home_players.append(player_data)
                else:
                    home_bench.append(player_data)
            
            # Add any extra bench players
            for player in lineups_data.get("home", {}).get("bench", []):
                home_bench.append(safe_player_data(player))
            
            # Process away team
            away_players = []
            away_bench = []
            
            for player in lineups_data.get("away", {}).get("players", []):
                player_data = safe_player_data(player)
                if player.get("lineup", True):
                    away_players.append(player_data)
                else:
                    away_bench.append(player_data)
            
            # Add any extra bench players
            for player in lineups_data.get("away", {}).get("bench", []):
                away_bench.append(safe_player_data(player))
            
            # Format payload
            payload = {
                "fixture_id": fixture["match_id"],
                "lineups": {
                    "home": {
                        "formation": str(lineups_data.get("home", {}).get("formation") or "4-2-3-1"),
                        "players": home_players,
                        "bench": home_bench,
                        "coach": {
                            "name": str(lineups_data.get("home", {}).get("coach", {}).get("name") or "Unknown"),
                            "country": lineups_data.get("home", {}).get("coach", {}).get("country")
                        }
                    },
                    "away": {
                        "formation": str(lineups_data.get("away", {}).get("formation") or "4-2-3-1"),
                        "players": away_players,
                        "bench": away_bench,
                        "coach": {
                            "name": str(lineups_data.get("away", {}).get("coach", {}).get("name") or "Unknown"),
                            "country": lineups_data.get("away", {}).get("coach", {}).get("country")
                        }
                    }
                },
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            # Log sample of extracted names
            if home_players:
                logger.info(f"📤 Sending {len(home_players)} home players, {len(away_players)} away players")
                logger.info(f"📋 Sample: #{home_players[0]['jerseyNumber']} - {home_players[0]['name']} ({home_players[0]['position']})")
            
            # Send to Rust backend
            response = std_requests.post(
                f"{FANCLASH_API}/games/lineups",
                json=payload,
                timeout=5,
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code == 200:
                logger.info(f"📋 Lineups forwarded successfully!")
                fixture["lineups_fetched"] = True
                return True
            else:
                logger.warning(f"⚠️ Rust returned {response.status_code}: {response.text[:200]}")
                return False
        else:
            logger.debug(f"⏳ Sofascore lineups returned {resp.status_code}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Failed to fetch lineups: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

def forward_to_rust(fixture: dict, event_type: str, data: dict):
    try:
        payload = {
            "fixture_id": fixture["match_id"],
            "event_type": event_type,
            "home_score": data.get("home_score", 0),
            "away_score": data.get("away_score", 0),
            "minute": data.get("time_elapsed", 0),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        std_requests.post(f"{FANCLASH_API}/games/live-update", json=payload, timeout=5)
    except Exception as e:
        logger.error(f"Failed to forward: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# LIVE GAME POLLING
# ─────────────────────────────────────────────────────────────────────────────

def poll_live_game(session: cffi_requests.Session, fixture: dict):
    sofascore_id = fixture.get("sofascore_id")
    if not sofascore_id:
        return
    
    auto_fetch_lineups(session, fixture)
    
    last_home = 0
    last_away = 0
    half_time_sent = False
    full_time_sent = False
    
    logger.info(f"🔴 STARTING LIVE POLLING: {fixture['home_team']} vs {fixture['away_team']}")
    
    while True:
        live_data = fetch_live_data(session, sofascore_id)
        if not live_data:
            time.sleep(POLL_INTERVAL_SEC)
            continue
        
        home_score = live_data["home_score"]
        away_score = live_data["away_score"]
        status_code = live_data["status_code"]
        status_type = live_data["status_type"]
        
        # Goals
        if home_score > last_home:
            logger.info(f"⚽ GOAL! {fixture['home_team']} - Score: {home_score}-{away_score}")
            forward_to_rust(fixture, "goal", live_data)
            last_home = home_score
        elif away_score > last_away:
            logger.info(f"⚽ GOAL! {fixture['away_team']} - Score: {home_score}-{away_score}")
            forward_to_rust(fixture, "goal", live_data)
            last_away = away_score
        
        # Half time
        if status_type == "pause" and not half_time_sent:
            logger.info(f"⏸ HALF TIME: {home_score}-{away_score}")
            forward_to_rust(fixture, "half_time", live_data)
            half_time_sent = True
        
        # Full time
        if status_code in (100, 110, 120) and not full_time_sent:
            logger.info(f"🏁 FULL TIME: {home_score}-{away_score}")
            forward_to_rust(fixture, "full_time", live_data)
            break
        
        time.sleep(POLL_INTERVAL_SEC)
    
    logger.info(f"✅ Finished polling {fixture['home_team']} vs {fixture['away_team']}")

# ─────────────────────────────────────────────────────────────────────────────
# MAIN LOOP - SIMPLE AND RELIABLE
# ─────────────────────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 55)
    logger.info("⚽ FanClash Live Poller - WORKING VERSION")
    logger.info("=" * 55)
    
    start_health_server()
    session = make_session()
    
    logger.info("🔄 Starting main polling loop...")
    
    while True:
        try:
            # Get fixtures
            fixtures = get_fixtures_from_rust()
            
            if not fixtures:
                logger.info("No fixtures found, sleeping 60 seconds...")
                time.sleep(60)
                continue
            
            now_utc = datetime.now(timezone.utc)
            now_eat = now_utc + NAIROBI_OFFSET
            
            # Check each fixture
            game_started = False
            
            for fixture in fixtures:
                ko_utc = fixture.get("_kickoff_utc")
                if not ko_utc:
                    continue
                
                mins_to_game = (ko_utc - now_utc).total_seconds() / 60
                mins_since_game = -mins_to_game
                
                # Display status for Chelsea game
                if "Chelsea" in fixture['home_team'] or "Chelsea" in fixture['away_team']:
                    logger.info(f"📊 Chelsea match: {mins_to_game:.0f} mins to kickoff, Status: {fixture['status']}")
                
                # Check if game is live or starting soon
                if mins_to_game <= 5 and fixture['status'] == 'upcoming':
                    logger.info(f"⚽ GAME STARTING SOON! {fixture['home_team']} vs {fixture['away_team']} at {mins_to_game:.0f} minutes")
                    # Update status to live in Rust? Or just start polling
                    fixture['status'] = 'live'
                
                # Game is live (within 5 mins before to 120 mins after)
                if mins_to_game <= 5 and fixture['status'] in ['upcoming', 'live']:
                    logger.info(f"🔴 LIVE GAME DETECTED! {fixture['home_team']} vs {fixture['away_team']}")
                    poll_live_game(session, fixture)
                    game_started = True
                    break
            
            # If no game is live or starting, sleep and check again
            if not game_started:
                # Find next game
                next_ko = None
                for fixture in fixtures:
                    ko_utc = fixture.get("_kickoff_utc")
                    if ko_utc and ko_utc > now_utc:
                        if not next_ko or ko_utc < next_ko:
                            next_ko = ko_utc
                
                if next_ko:
                    mins_to_next = (next_ko - now_utc).total_seconds() / 60
                    sleep_time = min(60, max(10, mins_to_next - 5))  # Sleep until 5 mins before
                    logger.info(f"💤 Next game in {mins_to_next:.0f} mins, sleeping {sleep_time:.0f} seconds...")
                    time.sleep(sleep_time)
                else:
                    logger.info("📭 No upcoming games, sleeping 60 seconds...")
                    time.sleep(60)
                    
        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            time.sleep(30)

if __name__ == "__main__":
    main()