"""
FanClash Live Score Poller — Complete Working Version with DB Updates
=======================================================================
Updates database with:
- Live scores
- Match status (upcoming → live → completed)
- Goal events (stored in array)
- Card events (yellow/red)
- Corner events
- Last polled timestamp
"""

import time
import logging
import sys
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

DATABASE_URL   = os.environ.get("DATABASE_URL",  "mongodb+srv://engineercapiyo_db_user:CapiyoClash1999@cluster0.omepeze.mongodb.net/clashdb?retryWrites=true&w=majority&appName=Cluster0")
FANCLASH_API   = os.environ.get("FANCLASH_API",  "https://fanclash-api.onrender.com/api")
SOFASCORE_API  = "https://api.sofascore.com/api/v1"
SOFASCORE_HOME = "https://www.sofascore.com"

NAIROBI_OFFSET    = timedelta(hours=3)
POLL_INTERVAL_SEC = 60
LIVE_WINDOW_MINS  = 120

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
# MONGODB
# ─────────────────────────────────────────────────────────────────────────────

def connect_db():
    client = MongoClient(
        DATABASE_URL,
        serverSelectionTimeoutMS=15000,
        connectTimeoutMS=15000,
        socketTimeoutMS=45000,
    )
    client.admin.command("ping")
    col = client["clashdb"]["games"]
    logger.info("✅ Connected to MongoDB")
    return client, col

def get_kickoff_utc(fixture: dict) -> Optional[datetime]:
    """Parse date_iso + time (EAT) → UTC datetime."""
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

def get_upcoming_fixtures(col) -> list:
    fixtures = list(col.find(
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

def get_live_fixtures(fixtures: list) -> list:
    now = datetime.now(timezone.utc)
    return [
        f for f in fixtures
        if -5 <= (now - f["_kickoff_utc"]).total_seconds() / 60 <= LIVE_WINDOW_MINS
    ]

def mark_newly_live_games(col):
    """Find upcoming games that have reached kickoff time and mark them as live"""
    now_utc = datetime.now(timezone.utc)
    upcoming = list(col.find({"status": "upcoming"}))
    
    marked = 0
    for game in upcoming:
        date_str = game.get("date_iso")
        time_str = game.get("time")
        if not date_str or not time_str or time_str == "TBD":
            continue
            
        try:
            kickoff_eat = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
            kickoff_utc = (kickoff_eat - NAIROBI_OFFSET).replace(tzinfo=timezone.utc)
            
            if now_utc >= kickoff_utc - timedelta(minutes=5):
                logger.info(f"🔴 MARKING LIVE: {game['home_team']} vs {game['away_team']}")
                col.update_one(
                    {"_id": game["_id"]},
                    {"$set": {
                        "status": "live",
                        "is_live": True,
                        "available_for_voting": False,
                        "live_started_at": datetime.now(timezone.utc).isoformat()
                    }}
                )
                marked += 1
        except Exception as e:
            logger.warning(f"Error parsing date: {e}")
    
    if marked > 0:
        logger.info(f"✅ Marked {marked} games as LIVE")
    return marked

# ─────────────────────────────────────────────────────────────────────────────
# USER FETCHING
# ─────────────────────────────────────────────────────────────────────────────

_all_users_cache = []
_cache_timestamp = None
CACHE_DURATION = timedelta(minutes=30)

def fetch_all_users() -> List[str]:
    global _all_users_cache, _cache_timestamp
    
    now = datetime.now()
    if _cache_timestamp and (now - _cache_timestamp) < CACHE_DURATION:
        return _all_users_cache
    
    users = []
    
    try:
        resp = std_requests.get(f"{FANCLASH_API}/users/all", timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, dict):
                users = data.get("users", []) or data.get("data", [])
            else:
                users = data
            users = [str(u.get("user_id") or u.get("id") or u) for u in users if u]
            logger.info(f"📊 Fetched {len(users)} users from /users/all")
    except Exception as e:
        logger.warning(f"⚠️ Could not fetch from /users/all: {e}")
    
    if not users:
        try:
            resp = std_requests.get(f"{FANCLASH_API}/votes/votes", timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                votes = data.get("data", []) if isinstance(data, dict) else data
                users = list({v.get("voterId", "") for v in votes if v.get("voterId")})
                logger.info(f"📊 Found {len(users)} unique users from votes")
        except Exception as e:
            logger.warning(f"⚠️ Could not fetch from votes: {e}")
    
    if not users:
        logger.warning("⚠️ No users found from API")
        users = []
    
    _all_users_cache = users
    _cache_timestamp = now
    return users

def fetch_voters(match_id: str) -> list:
    try:
        resp = std_requests.get(f"{FANCLASH_API}/votes/votes", timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            all_votes = data.get("data", []) if isinstance(data, dict) else data
            return [v for v in all_votes if v.get("fixtureId") == match_id]
    except Exception as e:
        logger.warning(f"⚠️ Could not fetch voters: {e}")
    return []

# ─────────────────────────────────────────────────────────────────────────────
# PUSH NOTIFICATIONS
# ─────────────────────────────────────────────────────────────────────────────

def send_push(user_id: str, title: str, body: str, ntype: str, data: dict):
    try:
        std_requests.post(
            f"{FANCLASH_API}/notifications/send",
            json={
                "user_id": user_id,
                "notification_type": ntype,
                "title": title,
                "body": body,
                "data": data,
            },
            timeout=10,
        )
    except Exception as e:
        logger.warning(f"⚠️ Push failed for {user_id}: {e}")

def notify_all_users(title: str, body: str, ntype: str, extra: dict = {}):
    users = fetch_all_users()
    if not users:
        logger.error(f"❌ No users found to notify for {ntype}")
        return False
    
    logger.info(f"📢 Sending '{ntype}' to {len(users)} users...")
    sent = 0
    for user_id in users:
        if user_id:
            send_push(user_id, title, body, ntype, extra)
            sent += 1
            time.sleep(0.05)
    
    logger.info(f"✅ [{ntype}] Sent to {sent} users")
    return sent > 0

def notify_voters_only(fixture: dict, title: str, body: str, ntype: str, extra: dict = {}):
    voters = fetch_voters(fixture["match_id"])
    if not voters:
        return
    
    seen = set()
    sent = 0
    for vote in voters:
        uid = vote.get("voterId", "")
        if not uid or uid in seen:
            continue
        seen.add(uid)
        send_push(uid, title, body, ntype, {
            "fixture_id": fixture["match_id"],
            "home_team": fixture["home_team"],
            "away_team": fixture["away_team"],
            **extra,
        })
        sent += 1
        time.sleep(0.05)
    
    if sent > 0:
        logger.info(f"📲 [{ntype}] → {sent} voters")

# ─────────────────────────────────────────────────────────────────────────────
# TEST NOTIFICATION
# ─────────────────────────────────────────────────────────────────────────────

def send_startup_test_notification():
    logger.info("=" * 55)
    logger.info("🔔 SENDING STARTUP TEST NOTIFICATION TO ALL USERS")
    logger.info("=" * 55)
    
    now_eat = (datetime.now(timezone.utc) + NAIROBI_OFFSET).strftime('%Y-%m-%d %H:%M:%S')
    
    success = notify_all_users(
        title="⚽ FanClash Live Poller is ACTIVE!",
        body=f"Your match notifications are now live. Time: {now_eat} EAT",
        ntype="test_startup",
        extra={"timestamp": now_eat, "test": True}
    )
    
    if success:
        logger.info("✅ TEST NOTIFICATION SENT SUCCESSFULLY!")
    else:
        logger.error("❌ TEST NOTIFICATION FAILED")
    
    logger.info("=" * 55)
    return success

# ─────────────────────────────────────────────────────────────────────────────
# LONG-TERM HYPE NOTIFICATIONS (ALL USERS)
# ─────────────────────────────────────────────────────────────────────────────

_sent_alerts: dict = {}

def _already_sent(match_id: str, alert: str) -> bool:
    return alert in _sent_alerts.get(match_id, set())

def _mark_sent(match_id: str, alert: str):
    _sent_alerts.setdefault(match_id, set()).add(alert)

def send_long_term_notifications(fixture: dict):
    if "_kickoff_utc" not in fixture:
        return
    
    now = datetime.now(timezone.utc)
    ko = fixture["_kickoff_utc"]
    days_to = (ko - now).total_seconds() / 86400
    match_id = fixture["match_id"]
    home = fixture["home_team"]
    away = fixture["away_team"]
    name = f"{home} vs {away}"
    ko_eat = (ko + NAIROBI_OFFSET).strftime('%A, %B %d at %H:%M')
    
    if 13 <= days_to <= 15 and not _already_sent(match_id, "t14d"):
        notify_all_users(
            title=f"🎉 2 weeks until {home} vs {away}!",
            body=f"Mark your calendar for {ko_eat} EAT. Rivalry starts now! ⚔️",
            ntype="hype_14_days",
            extra={"fixture_id": match_id, "days_to_kickoff": 14}
        )
        _mark_sent(match_id, "t14d")
    
    elif 6 <= days_to <= 8 and not _already_sent(match_id, "t7d"):
        notify_all_users(
            title=f"📅 1 week to go! {home} vs {away}",
            body=f"Kickoff at {ko_eat} EAT. Who's taking this? 🔥",
            ntype="hype_7_days",
            extra={"fixture_id": match_id, "days_to_kickoff": 7}
        )
        _mark_sent(match_id, "t7d")
    
    elif 0.8 <= days_to <= 1.2 and not _already_sent(match_id, "t1d"):
        notify_all_users(
            title=f"⏰ 24 hours until kick-off!",
            body=f"{name} tomorrow at {ko_eat} EAT. Final predictions? 🎯",
            ntype="hype_1_day",
            extra={"fixture_id": match_id, "days_to_kickoff": 1}
        )
        _mark_sent(match_id, "t1d")

def send_countdown_notifications(fixture: dict):
    if "_kickoff_utc" not in fixture:
        return
    
    now = datetime.now(timezone.utc)
    ko = fixture["_kickoff_utc"]
    mins_to = (ko - now).total_seconds() / 60
    match_id = fixture["match_id"]
    home = fixture["home_team"]
    away = fixture["away_team"]
    name = f"{home} vs {away}"
    ko_eat = (ko + NAIROBI_OFFSET).strftime("%H:%M")

    if 55 <= mins_to <= 65 and not _already_sent(match_id, "t60"):
        notify_all_users(
            title=f"🔔 1 hour until kick-off!",
            body=f"{name} kicks off at {ko_eat} EAT. Pick your side! ⚽",
            ntype="kickoff_reminder_60",
            extra={"fixture_id": match_id, "mins_to_kickoff": 60}
        )
        _mark_sent(match_id, "t60")

    elif 40 <= mins_to <= 50 and not _already_sent(match_id, "t45"):
        notify_all_users(
            title=f"⏰ 45 minutes to kick-off!",
            body=f"{name} at {ko_eat} EAT — get your votes in! 🎯",
            ntype="kickoff_reminder_45",
            extra={"fixture_id": match_id, "mins_to_kickoff": 45}
        )
        _mark_sent(match_id, "t45")

    elif 25 <= mins_to <= 35 and not _already_sent(match_id, "t30"):
        notify_all_users(
            title=f"⚡ 30 minutes to go!",
            body=f"{name} — rivalries heating up. Who's winning this? 🔥",
            ntype="kickoff_reminder_30",
            extra={"fixture_id": match_id, "mins_to_kickoff": 30}
        )
        _mark_sent(match_id, "t30")

    elif 5 <= mins_to <= 15 and not _already_sent(match_id, "t10"):
        notify_all_users(
            title=f"🔥 10 minutes! Last chance to vote!",
            body=f"{name} — locks soon. Don't miss out! ⏰",
            ntype="kickoff_reminder_10",
            extra={"fixture_id": match_id, "mins_to_kickoff": 10}
        )
        _mark_sent(match_id, "t10")

    elif -5 <= mins_to <= 5 and not _already_sent(match_id, "kickoff"):
        notify_all_users(
            title=f"⚽ We are LIVE!",
            body=f"{home} vs {away} has kicked off. May the best pick win! 🏆",
            ntype="kickoff_live",
            extra={"fixture_id": match_id, "mins_to_kickoff": 0}
        )
        _mark_sent(match_id, "kickoff")

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

def fetch_live_score(session: cffi_requests.Session, sofascore_id: int) -> Optional[dict]:
    try:
        time.sleep(1)
        resp = session.get(f"{SOFASCORE_API}/event/{sofascore_id}", timeout=15)
        if resp.status_code != 200:
            return None
        event = resp.json().get("event", {})
        return {
            "home_score": (event.get("homeScore") or {}).get("current"),
            "away_score": (event.get("awayScore") or {}).get("current"),
            "status_type": (event.get("status") or {}).get("type", ""),
            "status_code": (event.get("status") or {}).get("code", 0),
            "incidents": event.get("incidents", []),
            "venue": event.get("venue", {}),  # ← NEW: Venue info
            "time_elapsed": event.get("time", {}).get("elapsed", 0),  # ← NEW: Match minute
        }
    except Exception as e:
        logger.warning(f"⚠️ Error: {e}")
        return None

# ─────────────────────────────────────────────────────────────────────────────
# DB UPDATE FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def get_match_status(status_type: str, status_code: int) -> str:
    if status_type == "inprogress":
        return "live"
    if status_code in (100, 110, 120):
        return "completed"
    return "upcoming"

def detect_scorer(old: dict, new_data: dict) -> Optional[str]:
    old_home = old.get("home_score") or 0
    old_away = old.get("away_score") or 0
    new_home = new_data.get("home_score") or 0
    new_away = new_data.get("away_score") or 0
    if new_home > old_home:
        return "home_team"
    if new_away > old_away:
        return "away_team"
    return None

def update_fixture_score(col, fixture: dict, new_data: dict, scorer: Optional[str]):
    """UPDATE database with live scores and events"""
    new_status = get_match_status(new_data["status_type"], new_data["status_code"])
    
    # Build update object
    update_fields = {
        "home_score": new_data["home_score"],
        "away_score": new_data["away_score"],
        "status": new_status,
        "is_live": new_status == "live",
        "available_for_voting": new_status == "upcoming",
        "last_polled_at": datetime.now(timezone.utc).isoformat(),
        "time_elapsed": new_data.get("time_elapsed", 0),  # ← NEW: Match minute
    }
    
    # Add venue info if available and not already stored
    venue = new_data.get("venue", {})
    if venue and not fixture.get("venue"):
        update_fields["venue"] = venue.get("name", "")
        update_fields["venue_city"] = venue.get("city", "")
        update_fields["venue_country"] = venue.get("country", "")
    
    # If goal scored, add to goal_events array
    if scorer:
        update_fields["last_goal_scorer"] = scorer
        update_fields["last_goal_at"] = datetime.now(timezone.utc).isoformat()
        
        # Push to goal_events array for history
        col.update_one(
            {"match_id": fixture["match_id"]},
            {
                "$set": update_fields,
                "$push": {
                    "goal_events": {
                        "scorer": scorer,
                        "home_score": new_data["home_score"],
                        "away_score": new_data["away_score"],
                        "minute": new_data.get("time_elapsed", 0),
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }
                }
            }
        )
    else:
        col.update_one(
            {"match_id": fixture["match_id"]},
            {"$set": update_fields}
        )
    
    logger.info(f"💾 DB Updated: {fixture['home_team']} vs {fixture['away_team']} → {new_data['home_score']}-{new_data['away_score']} [{new_status}]")

def update_incidents_in_db(col, match_id: str, incidents: list):
    """Store incidents (cards, corners, offsides) in database"""
    for inc in incidents:
        inc_type = inc.get("incidentType", "").lower()
        inc_cls = inc.get("incidentClass", "").lower()
        is_home = inc.get("isHome", True)
        
        incident_data = {
            "type": inc_type,
            "class": inc_cls,
            "team": "home" if is_home else "away",
            "minute": inc.get("time", {}).get("elapsed", 0),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        
        col.update_one(
            {"match_id": match_id},
            {"$push": {"incidents": incident_data}}
        )

# ─────────────────────────────────────────────────────────────────────────────
# LIVE EVENT NOTIFICATIONS (VOTERS ONLY)
# ─────────────────────────────────────────────────────────────────────────────

def notify_goal_voters(fixture: dict, scorer: str, new_home: int, new_away: int):
    voters = fetch_voters(fixture["match_id"])
    if not voters:
        return

    seen = set()
    sent = 0
    for vote in voters:
        uid = vote.get("voterId", "")
        selection = vote.get("selection", "")
        if not uid or uid in seen:
            continue
        seen.add(uid)

        if selection == scorer:
            title = f"⚽ GOAL! Your team scored!"
            body = f"{fixture['home_team']} {new_home}-{new_away} {fixture['away_team']}"
            ntype = "goal_your_team"
        elif selection == "draw":
            title = f"⚽ Goal! Draw under pressure"
            body = f"{new_home}-{new_away} - Your draw prediction is shaky"
            ntype = "goal_draw_pressure"
        else:
            title = f"⚔️ RIVAL SCORED!"
            body = f"Your rival's team scored! {new_home}-{new_away}"
            ntype = "goal_rival_team"

        send_push(uid, title, body, ntype, {"fixture_id": fixture["match_id"]})
        sent += 1
        time.sleep(0.05)

    if sent > 0:
        logger.info(f"📲 [goal] {sent} voters")

def notify_half_time_voters(fixture: dict, home_score: int, away_score: int):
    notify_voters_only(fixture,
        title=f"⏸ Half Time: {home_score}-{away_score}",
        body=f"45 minutes remaining in {fixture['home_team']} vs {fixture['away_team']}",
        ntype="half_time",
        extra={"home_score": home_score, "away_score": away_score},
    )

def notify_full_time_voters(fixture: dict, home_score: int, away_score: int):
    match_id = fixture["match_id"]
    home_team = fixture["home_team"]
    away_team = fixture["away_team"]

    if home_score > away_score:
        result = f"{home_team} won!"
    elif away_score > home_score:
        result = f"{away_team} won!"
    else:
        result = "It's a draw!"

    voters = fetch_voters(match_id)
    if not voters:
        return

    seen = set()
    sent = 0
    for vote in voters:
        uid = vote.get("voterId", "")
        selection = vote.get("selection", "")
        if not uid or uid in seen:
            continue
        seen.add(uid)

        if (selection == "home_team" and home_score > away_score) or \
           (selection == "away_team" and away_score > home_score) or \
           (selection == "draw" and home_score == away_score):
            title = f"🏆 YOU CALLED IT!"
            body = f"{result} Final score: {home_score}-{away_score}"
            ntype = "full_time_win"
        else:
            title = f"😔 Final Whistle"
            body = f"{result} Final score: {home_score}-{away_score}"
            ntype = "full_time_loss"

        send_push(uid, title, body, ntype, {"fixture_id": match_id})
        sent += 1
        time.sleep(0.05)

    if sent > 0:
        logger.info(f"📲 [full_time] {sent} voters")

# ─────────────────────────────────────────────────────────────────────────────
# SMART SLEEP
# ─────────────────────────────────────────────────────────────────────────────

def smart_sleep(col):
    all_fixtures = get_upcoming_fixtures(col)
    if not all_fixtures:
        logger.info("📭 No upcoming fixtures — sleeping 1 hour")
        time.sleep(3600)
        return

    now = datetime.now(timezone.utc)
    future = [f["_kickoff_utc"] for f in all_fixtures if f["_kickoff_utc"] > now]
    if not future:
        logger.info("📭 No future kick-offs — sleeping 1 hour")
        time.sleep(3600)
        return

    next_ko = min(future)
    mins_to = (next_ko - now).total_seconds() / 60

    if mins_to <= 5:
        logger.info(f"⚽ Game starting soon — polling now")
        return
    elif mins_to <= 30:
        sleep_mins = min(5, max(1, mins_to - 1))
        logger.info(f"⏰ Game in {mins_to:.0f} mins — sleeping {sleep_mins:.0f} mins")
        time.sleep(sleep_mins * 60)
    elif mins_to <= 60:
        sleep_mins = mins_to - 30
        logger.info(f"💤 Game in {mins_to:.0f} mins — sleeping {sleep_mins:.0f} mins")
        time.sleep(sleep_mins * 60)
    else:
        sleep_mins = mins_to - 60
        logger.info(f"💤 Game in {mins_to:.0f} mins — sleeping {sleep_mins:.0f} mins")
        time.sleep(sleep_mins * 60)

# ─────────────────────────────────────────────────────────────────────────────
# CORE POLL LOOP
# ─────────────────────────────────────────────────────────────────────────────

def poll_live_fixtures(col, session: cffi_requests.Session, live_fixtures: list):
    watch = {f["match_id"]: f for f in live_fixtures}
    half_time_sent = {}
    full_time_sent = {}

    logger.info(f"🔴 Watching {len(watch)} live fixture(s)")

    while watch:
        for match_id, fixture in list(watch.items()):
            sofascore_id = fixture.get("sofascore_id")
            if not sofascore_id:
                watch.pop(match_id)
                continue

            live_data = fetch_live_score(session, sofascore_id)
            if not live_data:
                continue

            home_score = live_data.get("home_score") or 0
            away_score = live_data.get("away_score") or 0
            status_type = live_data["status_type"]
            status_code = live_data["status_code"]
            new_status = get_match_status(status_type, status_code)

            # Detect and update goals
            scorer = detect_scorer(fixture, live_data)
            if scorer:
                logger.info(f"⚽ GOAL! {fixture['home_team']} {home_score}-{away_score} {fixture['away_team']}")
                update_fixture_score(col, fixture, live_data, scorer)
                notify_goal_voters(fixture, scorer, home_score, away_score)
                
                # Refresh fixture data
                refreshed = col.find_one({"match_id": match_id})
                if refreshed:
                    refreshed["_kickoff_utc"] = fixture.get("_kickoff_utc")
                    watch[match_id] = refreshed
            else:
                # Still update scores even without new goal
                update_fixture_score(col, fixture, live_data, scorer=None)

            # Store incidents in DB
            incidents = live_data.get("incidents", [])
            if incidents:
                update_incidents_in_db(col, match_id, incidents)

            # Half time notification
            is_ht = (status_type == "pause" or status_code == 31)
            if is_ht and not half_time_sent.get(match_id):
                logger.info(f"⏸ Half time — {fixture['home_team']} vs {fixture['away_team']} {home_score}-{away_score}")
                notify_half_time_voters(fixture, home_score, away_score)
                half_time_sent[match_id] = True

            # Full time
            if new_status == "completed" and not full_time_sent.get(match_id):
                logger.info(f"🏁 Full time — {fixture['home_team']} vs {fixture['away_team']} {home_score}-{away_score}")
                notify_full_time_voters(fixture, home_score, away_score)
                full_time_sent[match_id] = True
                
                # Mark as completed in DB if not already
                col.update_one(
                    {"match_id": match_id},
                    {"$set": {
                        "status": "completed",
                        "is_live": False,
                        "completed_at": datetime.now(timezone.utc).isoformat()
                    }}
                )
                watch.pop(match_id)
                continue

        if watch:
            time.sleep(POLL_INTERVAL_SEC)

    logger.info("✅ All live games finished")

# ─────────────────────────────────────────────────────────────────────────────
# MAIN LOOP
# ─────────────────────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 55)
    logger.info("⚽ FanClash Live Poller - FULL VERSION with DB Updates")
    logger.info("📢 Long-term hype & countdown → ALL USERS")
    logger.info("📢 Live events (goals, HT, FT) → VOTERS ONLY")
    logger.info("💾 Updates DB: scores, status, venue, incidents, events")
    logger.info("=" * 55)

    start_health_server()
    mongo_client, col = connect_db()
    session = make_session()
    
    # Send test notification
    logger.info("")
    logger.info("🔔 SENDING TEST NOTIFICATION TO ALL USERS NOW...")
    send_startup_test_notification()
    time.sleep(5)
    
    logger.info("")
    logger.info("🔄 Starting main polling loop...")
    
    try:
        while True:
            try:
                # Mark games that should be live
                mark_newly_live_games(col)
                
                # Get all fixtures
                all_fixtures = get_upcoming_fixtures(col)
                
                # Send hype notifications (ALL USERS)
                for fixture in all_fixtures:
                    send_long_term_notifications(fixture)
                    send_countdown_notifications(fixture)
                
                # Poll live games
                live_now = get_live_fixtures(all_fixtures)
                if live_now:
                    logger.info(f"🔴 {len(live_now)} game(s) live — starting poller")
                    poll_live_fixtures(col, session, live_now)
                else:
                    smart_sleep(col)

            except KeyboardInterrupt:
                raise
            except Exception as e:
                logger.error(f"❌ Loop error: {e}", exc_info=True)
                time.sleep(60)

    except KeyboardInterrupt:
        logger.info("\n⏹️ Stopped by user")
    finally:
        mongo_client.close()
        logger.info("🔌 MongoDB connection closed")

if __name__ == "__main__":
    main()