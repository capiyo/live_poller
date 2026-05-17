"""
FanClash Live Score Poller - Complete Edition
Lineups, Events, and Statistics Support
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
STATS_INTERVAL_SEC = 60  # Send statistics every 60 seconds

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

# ─────────────────────────────────────────────────────────────────────────────
# LINEUP FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def auto_fetch_lineups(session: cffi_requests.Session, fixture: Dict[str, Any]) -> bool:
    sofascore_id = fixture.get("sofascore_id")
    if not sofascore_id or fixture.get("lineups_fetched"):
        return False
    
    try:
        resp = session.get(f"{SOFASCORE_API}/event/{sofascore_id}/lineups", timeout=10)
        if resp.status_code == 200:
            lineups_data = resp.json()
            
            def get_player_name(player):
                name_fields = ["name", "fullName", "displayName", "playerName", "shortName"]
                for field in name_fields:
                    if field in player and player[field]:
                        return str(player[field])
                if "player" in player:
                    player_obj = player["player"]
                    for field in name_fields:
                        if field in player_obj and player_obj[field]:
                            return str(player_obj[field])
                jersey = player.get("jerseyNumber", "Unknown")
                return f"Player #{jersey}"
            
            def safe_player_data(player):
                player_name = get_player_name(player)
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
                position = player.get("position") or "Unknown"
                return {
                    "name": player_name,
                    "position": str(position),
                    "jerseyNumber": jersey,
                    "captain": bool(player.get("captain", False)),
                    "lineup": bool(player.get("lineup", True)),
                }
            
            home_players = []
            home_bench = []
            for player in lineups_data.get("home", {}).get("players", []):
                player_data = safe_player_data(player)
                if player.get("lineup", True):
                    home_players.append(player_data)
                else:
                    home_bench.append(player_data)
            for player in lineups_data.get("home", {}).get("bench", []):
                home_bench.append(safe_player_data(player))
            
            away_players = []
            away_bench = []
            for player in lineups_data.get("away", {}).get("players", []):
                player_data = safe_player_data(player)
                if player.get("lineup", True):
                    away_players.append(player_data)
                else:
                    away_bench.append(player_data)
            for player in lineups_data.get("away", {}).get("bench", []):
                away_bench.append(safe_player_data(player))
            
            payload = {
                "fixture_id": fixture["match_id"],
                "lineups": {
                    "home": {
                        "formation": str(lineups_data.get("home", {}).get("formation") or "4-2-3-1"),
                        "players": home_players,
                        "bench": home_bench,
                        "coach": {
                            "name": str(lineups_data.get("home", {}).get("coach", {}).get("name") or "Unknown"),
                        }
                    },
                    "away": {
                        "formation": str(lineups_data.get("away", {}).get("formation") or "4-2-3-1"),
                        "players": away_players,
                        "bench": away_bench,
                        "coach": {
                            "name": str(lineups_data.get("away", {}).get("coach", {}).get("name") or "Unknown"),
                        }
                    }
                },
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
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
        return False
    except Exception as e:
        logger.error(f"Failed to fetch lineups: {e}")
        return False

# ─────────────────────────────────────────────────────────────────────────────
# EVENT FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def fetch_live_data(session: cffi_requests.Session, sofascore_id: int) -> Optional[dict]:
    try:
        time.sleep(0.5)
        resp = session.get(f"{SOFASCORE_API}/event/{sofascore_id}", timeout=15)
        if resp.status_code != 200:
            return None
        event = resp.json().get("event", {})
        
        incidents = event.get("incidents", [])
        
        # Fetch and parse statistics correctly
        statistics = {
            "ball_possession": {"home": 0, "away": 0},
            "total_shots": {"home": 0, "away": 0},
            "shots_on_target": {"home": 0, "away": 0},
            "corners": {"home": 0, "away": 0},
            "fouls": {"home": 0, "away": 0},
            "offsides": {"home": 0, "away": 0},
            "yellow_cards": {"home": 0, "away": 0},
            "red_cards": {"home": 0, "away": 0},
            "pass_accuracy": {"home": 0, "away": 0},
        }
        
        try:
            stats_resp = session.get(f"{SOFASCORE_API}/event/{sofascore_id}/statistics", timeout=10)
            if stats_resp.status_code == 200:
                stats_data = stats_resp.json()
                
                # Parse the nested statistics structure
                stats_list = stats_data.get("statistics", [])
                for period_data in stats_list:
                    groups = period_data.get("groups", [])
                    for group in groups:
                        items = group.get("statisticsItems", [])
                        for item in items:
                            name = item.get("name", "")
                            home_value = item.get("homeValue", 0)
                            away_value = item.get("awayValue", 0)
                            
                            # Convert to int (remove % if present)
                            if isinstance(home_value, str):
                                home_value = int(home_value.replace("%", ""))
                            if isinstance(away_value, str):
                                away_value = int(away_value.replace("%", ""))
                            
                            if "Ball possession" in name:
                                statistics["ball_possession"] = {"home": home_value, "away": away_value}
                            elif "Total shots" in name:
                                statistics["total_shots"] = {"home": home_value, "away": away_value}
                            elif "Shots on target" in name:
                                statistics["shots_on_target"] = {"home": home_value, "away": away_value}
                            elif "Corners" in name or "Corner kicks" in name:
                                statistics["corners"] = {"home": home_value, "away": away_value}
                            elif "Fouls" in name:
                                statistics["fouls"] = {"home": home_value, "away": away_value}
                            elif "Offsides" in name:
                                statistics["offsides"] = {"home": home_value, "away": away_value}
                            elif "Yellow cards" in name:
                                statistics["yellow_cards"] = {"home": home_value, "away": away_value}
                            elif "Red cards" in name:
                                statistics["red_cards"] = {"home": home_value, "away": away_value}
                            elif "Pass accuracy" in name:
                                statistics["pass_accuracy"] = {"home": home_value, "away": away_value}
                
                logger.info(f"📊 PARSED STATISTICS: Ball possession: {statistics['ball_possession']['home']}% - {statistics['ball_possession']['away']}%")
        except Exception as e:
            logger.warning(f"Failed to fetch statistics: {e}")
        
        return {
            "home_score": (event.get("homeScore") or {}).get("current", 0),
            "away_score": (event.get("awayScore") or {}).get("current", 0),
            "status_type": (event.get("status") or {}).get("type", ""),
            "status_code": (event.get("status") or {}).get("code", 0),
            "time_elapsed": event.get("time", {}).get("elapsed", 0),
            "time_extra": event.get("time", {}).get("extra", 0),
            "incidents": incidents,
            "statistics": statistics,
        }
    except Exception as e:
        logger.warning(f"Error fetching event: {e}")
        return None
def forward_event(fixture: dict, event_type: str, data: dict):
    payload = {
        "match_id": fixture["match_id"],
        "event_type": event_type,
        "minute": data.get("minute", 0),
        "minute_display": data.get("minute_display", ""),
        "home_score": data.get("home_score", 0),
        "away_score": data.get("away_score", 0),
        "player": data.get("player"),
        "team": data.get("team"),
        "player_out": data.get("player_out"),
        "player_in": data.get("player_in"),
        "shot_type": data.get("shot_type"),
        "on_target": data.get("on_target"),
        "blocked": data.get("blocked"),
    }
    payload = {k: v for k, v in payload.items() if v is not None}
    
    # DEBUG: Print the event payload
    logger.info(f"📤 EVENT PAYLOAD ({event_type}): {json.dumps(payload, indent=2)}")
    
    try:
        response = std_requests.post(
            f"{FANCLASH_API}/games/events",
            json=payload,
            timeout=5,
            headers={"Content-Type": "application/json"}
        )
        
        if response.status_code == 200:
            logger.info(f"✅ Forwarded {event_type} successfully - Response: {response.text[:100]}")
        else:
            logger.warning(f"❌ Failed to forward {event_type}: {response.status_code} - {response.text[:200]}")
            
    except Exception as e:
        logger.error(f"❌ Exception forwarding {event_type}: {e}")
def forward_statistics(fixture: dict, minute: int, minute_display: str, stats_data: dict, home_score: int, away_score: int):
    try:
        payload = {
            "match_id": fixture["match_id"],
            "minute": minute,
            "minute_display": minute_display,
            "home_score": home_score,
            "away_score": away_score,
            "ball_possession_home": stats_data.get("ball_possession", {}).get("home", 0),
            "ball_possession_away": stats_data.get("ball_possession", {}).get("away", 0),
            "total_shots_home": stats_data.get("total_shots", {}).get("home", 0),
            "total_shots_away": stats_data.get("total_shots", {}).get("away", 0),
            "shots_on_target_home": stats_data.get("shots_on_target", {}).get("home", 0),
            "shots_on_target_away": stats_data.get("shots_on_target", {}).get("away", 0),
            "corners_home": stats_data.get("corners", {}).get("home", 0),
            "corners_away": stats_data.get("corners", {}).get("away", 0),
            "fouls_home": stats_data.get("fouls", {}).get("home", 0),
            "fouls_away": stats_data.get("fouls", {}).get("away", 0),
            "offsides_home": stats_data.get("offsides", {}).get("home", 0),
            "offsides_away": stats_data.get("offsides", {}).get("away", 0),
            "yellow_cards_home": stats_data.get("yellow_cards", {}).get("home", 0),
            "yellow_cards_away": stats_data.get("yellow_cards", {}).get("away", 0),
            "red_cards_home": stats_data.get("red_cards", {}).get("home", 0),
            "red_cards_away": stats_data.get("red_cards", {}).get("away", 0),
            "pass_accuracy_home": stats_data.get("pass_accuracy", {}).get("home", 0),
            "pass_accuracy_away": stats_data.get("pass_accuracy", {}).get("away", 0),
        }
        
        # DEBUG: Print the payload
        logger.info(f"📊 STATS PAYLOAD: {json.dumps(payload)}")
        
        response = std_requests.post(
            f"{FANCLASH_API}/games/statistics",
            json=payload,
            timeout=5,
            headers={"Content-Type": "application/json"}
        )
        
        if response.status_code == 200:
            logger.info(f"📊 Statistics forwarded at {minute_display}")
        else:
            logger.warning(f"Failed to forward statistics: {response.status_code} - {response.text[:200]}")
            
    except Exception as e:
        logger.error(f"Failed to forward statistics: {e}")
def _get_player_name(inc: dict) -> str:
    if "player" in inc:
        player_obj = inc["player"]
        if isinstance(player_obj, dict):
            return player_obj.get("name") or player_obj.get("shortName") or "Unknown"
        return str(player_obj)
    return inc.get("name", "Unknown")

def _find_goal_scorer(incidents: list, is_home: bool) -> str:
    for inc in incidents:
        inc_type = inc.get("incidentType", "").lower()
        if inc_type == "goal" and inc.get("isHome") == is_home:
            return _get_player_name(inc)
    return "Unknown"

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
    seen_incidents = set()
    last_stats_minute = -STATS_INTERVAL_SEC
    
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
        time_elapsed = live_data["time_elapsed"]
        time_extra = live_data.get("time_extra", 0)
        incidents = live_data.get("incidents", [])
        statistics = live_data.get("statistics", {})
        
        minute_display = f"{time_elapsed}" + (f"+{time_extra}" if time_extra > 0 else "")
        
        # FORWARD GOALS
        if home_score > last_home:
            logger.info(f"⚽ GOAL! {fixture['home_team']} - Score: {home_score}-{away_score} ({minute_display}')")
            forward_event(fixture, "goal", {
                "minute": time_elapsed,
                "minute_display": minute_display,
                "home_score": home_score,
                "away_score": away_score,
                "team": fixture['home_team'],
                "player": _find_goal_scorer(incidents, is_home=True)
            })
            last_home = home_score
        elif away_score > last_away:
            logger.info(f"⚽ GOAL! {fixture['away_team']} - Score: {home_score}-{away_score} ({minute_display}')")
            forward_event(fixture, "goal", {
                "minute": time_elapsed,
                "minute_display": minute_display,
                "home_score": home_score,
                "away_score": away_score,
                "team": fixture['away_team'],
                "player": _find_goal_scorer(incidents, is_home=False)
            })
            last_away = away_score
        
        # FORWARD INCIDENTS (Cards, Substitutions, Shots, Fouls, etc.)
        for inc in incidents:
            inc_id = str(inc.get("id", ""))
            if inc_id in seen_incidents:
                continue
            seen_incidents.add(inc_id)
            
            inc_type = inc.get("incidentType", "").lower()
            inc_cls = inc.get("incidentClass", "").lower()
            is_home = inc.get("isHome", True)
            team = fixture["home_team"] if is_home else fixture["away_team"]
            minute = inc.get("time", {}).get("elapsed", time_elapsed)
            extra = inc.get("time", {}).get("extra", 0)
            minute_disp = f"{minute}" + (f"+{extra}" if extra > 0 else "")
            player = _get_player_name(inc)
            
            if inc_type == "card":
                if inc_cls == "yellow":
                    logger.info(f"🟨 YELLOW CARD - {team}: {player} ({minute_disp}')")
                    forward_event(fixture, "yellow_card", {
                        "minute": minute, "minute_display": minute_disp,
                        "player": player, "team": team
                    })
                elif inc_cls == "red":
                    logger.info(f"🟥 RED CARD - {team}: {player} ({minute_disp}')")
                    forward_event(fixture, "red_card", {
                        "minute": minute, "minute_display": minute_disp,
                        "player": player, "team": team
                    })
            elif inc_type == "substitution":
                player_out = _get_player_name(inc.get("playerOut", {}))
                player_in = _get_player_name(inc.get("playerIn", {}))
                logger.info(f"🔄 SUBSTITUTION - {team}: {player_out} → {player_in} ({minute_disp}')")
                forward_event(fixture, "substitution", {
                    "minute": minute, "minute_display": minute_disp,
                    "player_out": player_out, "player_in": player_in, "team": team
                })
            elif inc_type == "shot":
                on_target = inc.get("onTarget", False)
                blocked = inc.get("blocked", False)
                logger.info(f"🎯 SHOT - {player} ({team}) - {'on target' if on_target else 'off target'} ({minute_disp}')")
                forward_event(fixture, "shot", {
                    "minute": minute, "minute_display": minute_disp,
                    "player": player, "team": team,
                    "on_target": on_target, "blocked": blocked
                })
            elif inc_type == "foul":
                logger.info(f"⚠️ FOUL - {team}: {player} ({minute_disp}')")
                forward_event(fixture, "foul", {
                    "minute": minute, "minute_display": minute_disp,
                    "player": player, "team": team
                })
            elif inc_type == "corner":
                logger.info(f"🚩 CORNER - {team} ({minute_disp}')")
                forward_event(fixture, "corner", {
                    "minute": minute, "minute_display": minute_disp, "team": team
                })
            elif inc_type == "offside":
                logger.info(f"🚩 OFFSIDE - {team}: {player} ({minute_disp}')")
                forward_event(fixture, "offside", {
                    "minute": minute, "minute_display": minute_disp,
                    "player": player, "team": team
                })
        
        if statistics and time_elapsed > 0 and time_elapsed - last_stats_minute >= STATS_INTERVAL_SEC:
           forward_statistics(fixture, time_elapsed, minute_display, statistics, home_score, away_score)
           last_stats_minute = time_elapsed
        
        # HALF TIME
        if status_type == "pause" and not half_time_sent:
            logger.info(f"⏸ HALF TIME: {home_score}-{away_score}")
            forward_event(fixture, "half_time", {
                "minute": time_elapsed, "minute_display": f"{time_elapsed}'",
                "home_score": home_score, "away_score": away_score
            })
            half_time_sent = True
        
        # FULL TIME
        if status_code in (100, 110, 120) and not full_time_sent:
            logger.info(f"🏁 FULL TIME: {home_score}-{away_score}")
            forward_event(fixture, "match_end", {
                "minute": time_elapsed, "minute_display": f"{time_elapsed}'",
                "home_score": home_score, "away_score": away_score
            })
            full_time_sent = True
            break
        
        # SECOND HALF STARTED
        if status_type == "inprogress" and half_time_sent and not hasattr(poll_live_game, '_second_half_sent'):
            logger.info(f"▶️ SECOND HALF STARTED")
            forward_event(fixture, "second_half", {
                "minute": time_elapsed, "minute_display": f"{time_elapsed}'"
            })
            poll_live_game._second_half_sent = True
        
        time.sleep(POLL_INTERVAL_SEC)
    
    logger.info(f"✅ Finished polling {fixture['home_team']} vs {fixture['away_team']}")

# ─────────────────────────────────────────────────────────────────────────────
# MAIN LOOP
# ─────────────────────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 55)
    logger.info("⚽ FanClash Live Poller - Complete Edition")
    logger.info("📋 Lineups + 📅 Events + 📊 Statistics")
    logger.info("=" * 55)
    
    start_health_server()
    session = make_session()
    
    logger.info("🔄 Starting main polling loop...")
    
    while True:
        try:
            fixtures = get_fixtures_from_rust()
            
            if not fixtures:
                logger.info("No fixtures found, sleeping 60 seconds...")
                time.sleep(60)
                continue
            
            now_utc = datetime.now(timezone.utc)
            game_started = False
            
            for fixture in fixtures:
                ko_utc = fixture.get("_kickoff_utc")
                if not ko_utc:
                    continue
                
                mins_to_game = (ko_utc - now_utc).total_seconds() / 60
                
                if "Chelsea" in fixture['home_team'] or "Chelsea" in fixture['away_team']:
                    logger.info(f"📊 Chelsea match: {mins_to_game:.0f} mins to kickoff")
                
                if mins_to_game <= 5 and fixture['status'] in ['upcoming', 'live']:
                    logger.info(f"🔴 LIVE GAME DETECTED! {fixture['home_team']} vs {fixture['away_team']}")
                    poll_live_game(session, fixture)
                    game_started = True
                    break
            
            if not game_started:
                next_ko = None
                for fixture in fixtures:
                    ko_utc = fixture.get("_kickoff_utc")
                    if ko_utc and ko_utc > now_utc:
                        if not next_ko or ko_utc < next_ko:
                            next_ko = ko_utc
                
                if next_ko:
                    mins_to_next = (next_ko - now_utc).total_seconds() / 60
                    sleep_time = min(60, max(10, mins_to_next - 5))
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