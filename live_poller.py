"""
FanClash Live Score Poller — Render Free Tier Edition
======================================================
Runs as a Render Web Service (free tier) by opening a tiny
health-check HTTP server on the required port. The actual
poller logic runs on a background thread alongside it.

Notification timeline:
  T-60  → "🔔 Kick-off in 1 hour"
  T-30  → "⚡ 30 mins to go"
  T-10  → "🔥 Last chance to vote"
  T+0   → "⚽ We are LIVE"
  GOAL       → personalised (your team / rival / draw)
  YELLOW     → "🟨 Yellow card — {team}"
  CORNER     → "🚩 Corner to {team}"
  OFFSIDE    → "🚩 Offside — {team}"
  HALF TIME  → "⏸ Half time — {score}"
  FULL TIME  → "🏁 You called it / Not your day"
"""

import time
import logging
import sys
import os
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timedelta, timezone
from typing import Optional

from curl_cffi import requests as cffi_requests
import requests as std_requests
from pymongo import MongoClient

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

DATABASE_URL   = os.environ.get("DATABASE_URL",  "mongodb+srv://Capiyo:Capiyo%401010@cluster0.22lay5z.mongodb.net/clashdb?retryWrites=true&w=majority&appName=Cluster0")
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
# HEALTH SERVER  (keeps Render free web service alive)
# ─────────────────────────────────────────────────────────────────────────────

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"FanClash Poller OK")

    def log_message(self, *args):
        pass  # silence noisy access logs


def start_health_server():
    """Start tiny HTTP server on Render's required PORT in a daemon thread."""
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
        logger.warning(f"⚠️  Could not parse kick-off: {e}")
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


def get_next_kickoff(fixtures: list) -> Optional[datetime]:
    now = datetime.now(timezone.utc)
    future = [f["_kickoff_utc"] for f in fixtures if f["_kickoff_utc"] > now]
    return min(future) if future else None

# ─────────────────────────────────────────────────────────────────────────────
# SMART SLEEP
# ─────────────────────────────────────────────────────────────────────────────

def smart_sleep(col):
    """
    Sleep until 60 mins before the next kick-off.
    Caps at 1 hour so we re-check for newly scraped fixtures regularly.
    """
    all_fixtures = get_upcoming_fixtures(col)

    if not all_fixtures:
        logger.info("📭 No upcoming fixtures — sleeping 6 hours")
        time.sleep(21600)
        return

    now     = datetime.now(timezone.utc)
    next_ko = get_next_kickoff(all_fixtures)

    if not next_ko:
        logger.info("📭 No future kick-offs — sleeping 6 hours")
        time.sleep(21600)
        return

    wake_at    = next_ko - timedelta(minutes=60)
    eat_ko     = next_ko + NAIROBI_OFFSET
    sleep_secs = (wake_at - now).total_seconds()

    if sleep_secs <= 0:
        return  # already past T-60 — check immediately

    sleep_secs = min(sleep_secs, 3600)  # max 1 hour per sleep
    logger.info(
        f"💤 Next game: {eat_ko.strftime('%Y-%m-%d %H:%M')} EAT — "
        f"sleeping {sleep_secs/3600:.1f}h"
    )
    time.sleep(sleep_secs)

# ─────────────────────────────────────────────────────────────────────────────
# SOFASCORE
# ─────────────────────────────────────────────────────────────────────────────

def make_session() -> cffi_requests.Session:
    session = cffi_requests.Session(impersonate="chrome124")
    session.headers.update({"Accept-Language": "en-US,en;q=0.9"})
    try:
        session.get(SOFASCORE_HOME, timeout=15)
    except Exception:
        pass
    session.headers.update({
        "Accept":  "application/json, text/plain, */*",
        "Referer": f"{SOFASCORE_HOME}/",
        "Origin":  SOFASCORE_HOME,
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
            "home_score":  (event.get("homeScore") or {}).get("current"),
            "away_score":  (event.get("awayScore") or {}).get("current"),
            "status_type": (event.get("status") or {}).get("type", ""),
            "status_code": (event.get("status") or {}).get("code", 0),
            "incidents":   event.get("incidents", []),
        }
    except Exception as e:
        logger.warning(f"⚠️  Error fetching event {sofascore_id}: {e}")
        return None

# ─────────────────────────────────────────────────────────────────────────────
# PUSH NOTIFICATIONS — core sender
# ─────────────────────────────────────────────────────────────────────────────

def send_push(user_id: str, title: str, body: str, ntype: str, data: dict):
    """POST to Rust /api/notifications/send — matches SendNotificationRequest."""
    try:
        std_requests.post(
            f"{FANCLASH_API}/notifications/send",
            json={
                "user_id":           user_id,
                "notification_type": ntype,
                "title":             title,
                "body":              body,
                "data":              data,
            },
            timeout=10,
        )
    except Exception as e:
        logger.warning(f"⚠️  Push failed for {user_id}: {e}")


def fetch_voters(match_id: str) -> list:
    try:
        resp = std_requests.get(f"{FANCLASH_API}/votes/votes", timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            all_votes = data.get("data", []) if isinstance(data, dict) else data
            return [v for v in all_votes if v.get("fixtureId") == match_id]
    except Exception as e:
        logger.warning(f"⚠️  Could not fetch voters: {e}")
    return []


def notify_all_voters(fixture: dict, title: str, body: str, ntype: str, extra: dict = {}):
    """Same notification to every unique voter of a fixture."""
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
            "home_team":  fixture["home_team"],
            "away_team":  fixture["away_team"],
            **extra,
        })
        sent += 1
        time.sleep(0.05)
    logger.info(f"📲 [{ntype}] → {sent} voters | {fixture['home_team']} vs {fixture['away_team']}")

# ─────────────────────────────────────────────────────────────────────────────
# COUNTDOWN NOTIFICATIONS  (T-60 / T-30 / T-10 / kick-off)
# ─────────────────────────────────────────────────────────────────────────────

_sent_alerts: dict = {}  # match_id → set of alert names already sent


def _already_sent(match_id: str, alert: str) -> bool:
    return alert in _sent_alerts.get(match_id, set())


def _mark_sent(match_id: str, alert: str):
    _sent_alerts.setdefault(match_id, set()).add(alert)


def send_countdown_notifications(fixture: dict):
    now      = datetime.now(timezone.utc)
    ko       = fixture["_kickoff_utc"]
    mins_to  = (ko - now).total_seconds() / 60
    match_id = fixture["match_id"]
    home     = fixture["home_team"]
    away     = fixture["away_team"]
    name     = f"{home} vs {away}"
    ko_eat   = (ko + NAIROBI_OFFSET).strftime("%H:%M")

    if 55 <= mins_to <= 65 and not _already_sent(match_id, "t60"):
        notify_all_voters(fixture,
            title=f"🔔 1 hour until kick-off!",
            body=f"{name} kicks off at {ko_eat} EAT. Pick your side! ⚽",
            ntype="kickoff_reminder_60",
            extra={"mins_to_kickoff": 60},
        )
        _mark_sent(match_id, "t60")

    elif 25 <= mins_to <= 35 and not _already_sent(match_id, "t30"):
        notify_all_voters(fixture,
            title=f"⚡ 30 minutes to go!",
            body=f"{name} — rivalries heating up. Who's winning this? 🔥",
            ntype="kickoff_reminder_30",
            extra={"mins_to_kickoff": 30},
        )
        _mark_sent(match_id, "t30")

    elif 5 <= mins_to <= 15 and not _already_sent(match_id, "t10"):
        notify_all_voters(fixture,
            title=f"🔥 10 minutes! Last chance to vote!",
            body=f"{name} — locks soon. Don't miss out! ⏰",
            ntype="kickoff_reminder_10",
            extra={"mins_to_kickoff": 10},
        )
        _mark_sent(match_id, "t10")

    elif -5 <= mins_to <= 5 and not _already_sent(match_id, "kickoff"):
        notify_all_voters(fixture,
            title=f"⚽ We are LIVE!",
            body=f"{home} vs {away} has kicked off. May the best pick win! 🏆",
            ntype="kickoff_live",
            extra={"mins_to_kickoff": 0},
        )
        _mark_sent(match_id, "kickoff")


def run_countdown_for_upcoming(col, upcoming_fixtures: list):
    now  = datetime.now(timezone.utc)
    soon = [
        f for f in upcoming_fixtures
        if 0 < (f["_kickoff_utc"] - now).total_seconds() / 60 <= 65
    ]
    for fixture in soon:
        send_countdown_notifications(fixture)

# ─────────────────────────────────────────────────────────────────────────────
# LIVE EVENT NOTIFICATIONS
# ─────────────────────────────────────────────────────────────────────────────

def notify_goal(fixture: dict, scorer: str, new_home: int, new_away: int):
    match_id    = fixture["match_id"]
    home_team   = fixture["home_team"]
    away_team   = fixture["away_team"]
    name        = f"{home_team} vs {away_team}"
    score_line  = f"{new_home}-{new_away}"
    scored_team = home_team if scorer == "home_team" else away_team
    now_iso     = datetime.now(timezone.utc).isoformat()

    voters = fetch_voters(match_id)
    if not voters:
        return

    seen = set()
    sent = 0
    for vote in voters:
        uid       = vote.get("voterId", "")
        selection = vote.get("selection", "")
        if not uid or uid in seen:
            continue
        seen.add(uid)

        if selection == scorer:
            title = f"⚽ {scored_team} scored!"
            body  = f"{name} → {score_line}. Your pick is winning! 😤"
            ntype = "goal_your_team"
        elif selection == "draw":
            title = f"⚽ Goal! {scored_team} score"
            body  = f"{name} → {score_line}. Your draw is under pressure 😬"
            ntype = "goal_draw_pressure"
        else:
            title = f"⚔️ {scored_team} scored against you!"
            body  = f"{name} → {score_line}. Rivals are coming for you! 😈"
            ntype = "goal_rival_team"

        send_push(uid, title, body, ntype, {
            "fixture_id": match_id,
            "scorer":     scorer,
            "home_score": new_home,
            "away_score": new_away,
            "home_team":  home_team,
            "away_team":  away_team,
            "timestamp":  now_iso,
        })
        sent += 1
        time.sleep(0.05)

    logger.info(f"📲 [goal] {sent} voters — {name} {score_line}")


def notify_half_time(fixture: dict, home_score: int, away_score: int):
    score = f"{home_score}-{away_score}"
    notify_all_voters(fixture,
        title=f"⏸ Half time — {score}",
        body=f"{fixture['home_team']} vs {fixture['away_team']} — 45 more mins. Still alive? 👀",
        ntype="half_time",
        extra={"home_score": home_score, "away_score": away_score},
    )


def notify_full_time(fixture: dict, home_score: int, away_score: int):
    match_id  = fixture["match_id"]
    home_team = fixture["home_team"]
    away_team = fixture["away_team"]
    name      = f"{home_team} vs {away_team}"
    score     = f"{home_score}-{away_score}"
    now_iso   = datetime.now(timezone.utc).isoformat()

    if home_score > away_score:
        actual = "home_team"
    elif away_score > home_score:
        actual = "away_team"
    else:
        actual = "draw"

    voters = fetch_voters(match_id)
    if not voters:
        return

    seen = set()
    sent = 0
    for vote in voters:
        uid       = vote.get("voterId", "")
        selection = vote.get("selection", "")
        if not uid or uid in seen:
            continue
        seen.add(uid)

        if selection == actual:
            title = f"🏆 You called it!"
            body  = f"{name} {score} — your rivals owe you respect 😎"
            ntype = "full_time_win"
        else:
            title = f"😔 Not your day"
            body  = f"{name} {score} — better luck next match! 💪"
            ntype = "full_time_loss"

        send_push(uid, title, body, ntype, {
            "fixture_id":    match_id,
            "home_score":    home_score,
            "away_score":    away_score,
            "actual_result": actual,
            "home_team":     home_team,
            "away_team":     away_team,
            "timestamp":     now_iso,
        })
        sent += 1
        time.sleep(0.05)

    logger.info(f"📲 [full_time] {sent} voters — {name} {score}")

# ─────────────────────────────────────────────────────────────────────────────
# INCIDENT TRACKER  (yellow / corner / offside)
# ─────────────────────────────────────────────────────────────────────────────

_seen_incidents: dict = {}  # match_id → set of incident IDs already processed


def process_incidents(fixture: dict, incidents: list, home_score: int, away_score: int):
    match_id   = fixture["match_id"]
    home_team  = fixture["home_team"]
    away_team  = fixture["away_team"]
    score_line = f"{home_score}-{away_score}"
    seen       = _seen_incidents.setdefault(match_id, set())

    for inc in incidents:
        inc_id   = str(inc.get("id") or id(inc))
        inc_type = (inc.get("incidentType") or "").lower()
        inc_cls  = (inc.get("incidentClass") or "").lower()
        is_home  = inc.get("isHome", True)
        team     = home_team if is_home else away_team

        if inc_id in seen:
            continue
        seen.add(inc_id)

        if inc_type == "card" and inc_cls == "yellow":
            logger.info(f"🟨 Yellow card — {team}")
            notify_all_voters(fixture,
                title=f"🟨 Yellow card — {team}",
                body=f"{home_team} vs {away_team} → {score_line}. Things heating up! 🔥",
                ntype="yellow_card",
                extra={"team": team, "score": score_line},
            )

        elif inc_type == "corner":
            logger.info(f"🚩 Corner — {team}")
            notify_all_voters(fixture,
                title=f"🚩 Corner to {team}",
                body=f"{home_team} vs {away_team} → {score_line}. Dangerous set piece! 😤",
                ntype="corner",
                extra={"team": team, "score": score_line},
            )

        elif inc_type == "offside":
            logger.info(f"🚩 Offside — {team}")
            notify_all_voters(fixture,
                title=f"🚩 Offside — {team}",
                body=f"{home_team} vs {away_team} → {score_line}",
                ntype="offside",
                extra={"team": team, "score": score_line},
            )

# ─────────────────────────────────────────────────────────────────────────────
# DB UPDATES
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
    new_status = get_match_status(new_data["status_type"], new_data["status_code"])
    update = {
        "$set": {
            "home_score":           new_data["home_score"],
            "away_score":           new_data["away_score"],
            "status":               new_status,
            "is_live":              new_status == "live",
            "available_for_voting": new_status == "upcoming",
        }
    }
    if scorer:
        update["$push"] = {
            "goal_events": {
                "scorer":     scorer,
                "home_score": new_data["home_score"],
                "away_score": new_data["away_score"],
                "timestamp":  datetime.now(timezone.utc).isoformat(),
            }
        }
    col.update_one({"match_id": fixture["match_id"]}, update)
    logger.info(
        f"💾 {fixture['home_team']} vs {fixture['away_team']} "
        f"→ {new_data['home_score']}-{new_data['away_score']} [{new_status}]"
    )


def resolve_first_goal_prop(col, fixture: dict, scorer: str):
    if (fixture.get("home_score") or 0) == 0 and (fixture.get("away_score") or 0) == 0:
        match_id = fixture["match_id"]
        col.database["sub_fixture_results"].update_one(
            {"sub_fixture_id": f"goal_{match_id}"},
            {"$set": {
                "sub_fixture_id": f"goal_{match_id}",
                "result":         scorer,
                "resolved_at":    datetime.now(timezone.utc).isoformat(),
                "match_id":       match_id,
            }},
            upsert=True,
        )
        logger.info(f"🏆 First goal prop resolved → {scorer}")

# ─────────────────────────────────────────────────────────────────────────────
# CORE POLL LOOP
# ─────────────────────────────────────────────────────────────────────────────

def poll_live_fixtures(col, session: cffi_requests.Session, live_fixtures: list):
    watch          = {f["match_id"]: f for f in live_fixtures}
    half_time_sent = {}

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

            home_score  = live_data.get("home_score") or 0
            away_score  = live_data.get("away_score") or 0
            status_type = live_data["status_type"]
            status_code = live_data["status_code"]
            new_status  = get_match_status(status_type, status_code)

            # Countdown alerts (kick-off notification fires here too)
            send_countdown_notifications(fixture)

            # Goal
            scorer = detect_scorer(fixture, live_data)
            if scorer:
                logger.info(f"⚽ GOAL! {fixture['home_team']} vs {fixture['away_team']} → {home_score}-{away_score}")
                update_fixture_score(col, fixture, live_data, scorer)
                resolve_first_goal_prop(col, fixture, scorer)
                notify_goal(fixture, scorer, home_score, away_score)
                refreshed = col.find_one({"match_id": match_id})
                if refreshed:
                    refreshed["_kickoff_utc"] = fixture.get("_kickoff_utc")
                    watch[match_id] = refreshed
            else:
                update_fixture_score(col, fixture, live_data, scorer=None)

            # Incidents
            incidents = live_data.get("incidents", [])
            if incidents:
                process_incidents(fixture, incidents, home_score, away_score)

            # Half time (Sofascore: status_type="pause" or status_code=31)
            is_ht = (status_type == "pause" or status_code == 31)
            if is_ht and not half_time_sent.get(match_id):
                logger.info(f"⏸  Half time — {fixture['home_team']} vs {fixture['away_team']} {home_score}-{away_score}")
                notify_half_time(fixture, home_score, away_score)
                half_time_sent[match_id] = True

            # Full time
            if new_status == "completed":
                logger.info(f"🏁 Full time — {fixture['home_team']} vs {fixture['away_team']} {home_score}-{away_score}")
                notify_full_time(fixture, home_score, away_score)
                watch.pop(match_id)
                continue

        if watch:
            logger.info(f"⏳ {len(watch)} game(s) live — sleeping {POLL_INTERVAL_SEC}s")
            time.sleep(POLL_INTERVAL_SEC)

    logger.info("✅ All live games finished")

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 55)
    logger.info("⚽  FanClash Live Poller — Match Day Edition")
    logger.info("🌐  Running as Render Web Service (free tier)")
    logger.info("=" * 55)

    # Start health server first so Render sees an open port immediately
    start_health_server()

    mongo_client, col = connect_db()
    session = make_session()

    try:
        while True:
            try:
                all_fixtures = get_upcoming_fixtures(col)

                # Send countdown notifications for games within 65 mins
                run_countdown_for_upcoming(col, all_fixtures)

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
        logger.info("\n⏹️  Stopped by user")
    finally:
        mongo_client.close()
        logger.info("🔌 MongoDB connection closed")


if __name__ == "__main__":
    main()