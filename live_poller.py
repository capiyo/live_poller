"""
FanClash Live Score Poller — Full Match Day Edition
=====================================================
Notification timeline per fixture:

  T-60 mins  → "🔔 Kick-off in 1 hour"        (all voters)
  T-30 mins  → "⚡ 30 mins to go"              (all voters)
  T-10 mins  → "🔥 10 mins! Last chance to vote" (unvoted users too)
  T+0        → "⚽ We are LIVE"                (all voters)

  ── LIVE ────────────────────────────────────────────────────────
  GOAL       → personalised (your team / rival team / draw)
  YELLOW     → "🟨 Yellow card — {team}"
  CORNER     → "🚩 Corner to {team}"
  OFFSIDE    → "🚩 Offside — {team}"
  HALF TIME  → "⏸ Half time — {score}"
  FULL TIME  → "🏁 Full time — you were right/wrong + result"

Deploy:  Render Background Worker
Start:   python live_poller.py
"""

import time
import logging
import sys
import os
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
POLL_INTERVAL_SEC = 60    # seconds between score checks while live
LIVE_WINDOW_MINS  = 120   # treat a match as possibly live for this long after kick-off

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
    Wake up every hour to re-check in case new fixtures were scraped.
    """
    all_fixtures = get_upcoming_fixtures(col)

    if not all_fixtures:
        logger.info("📭 No upcoming fixtures — sleeping 6 hours")
        time.sleep(21600)
        return

    now = datetime.now(timezone.utc)
    next_ko = get_next_kickoff(all_fixtures)

    if not next_ko:
        logger.info("📭 No future kick-offs — sleeping 6 hours")
        time.sleep(21600)
        return

    # Wake at T-60 to send the first notification
    wake_at = next_ko - timedelta(minutes=60)
    eat_ko  = next_ko + NAIROBI_OFFSET

    if wake_at <= now:
        # Already past T-60 — wake up immediately
        return

    sleep_secs = (wake_at - now).total_seconds()
    # Cap single sleep at 1 hour so we re-check for new fixtures regularly
    sleep_secs = min(sleep_secs, 3600)

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
            # incident data for yellow/corner/offside
            "incidents":   event.get("incidents", []),
        }
    except Exception as e:
        logger.warning(f"⚠️  Error fetching event {sofascore_id}: {e}")
        return None

# ─────────────────────────────────────────────────────────────────────────────
# NOTIFICATIONS — core sender
# ─────────────────────────────────────────────────────────────────────────────

def send_push(user_id: str, title: str, body: str, ntype: str, data: dict):
    """POST to /api/notifications/send — matches your Rust SendNotificationRequest."""
    try:
        payload = {
            "user_id":           user_id,       # Rust field: user_id
            "notification_type": ntype,          # Rust field: notification_type
            "title":             title,
            "body":              body,
            "data":              data,
        }
        std_requests.post(
            f"{FANCLASH_API}/notifications/send",
            json=payload,
            timeout=10,
        )
    except Exception as e:
        logger.warning(f"⚠️  Push failed for {user_id}: {e}")


def fetch_voters(match_id: str) -> list:
    """Fetch all votes for a fixture."""
    try:
        resp = std_requests.get(f"{FANCLASH_API}/votes/votes", timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            all_votes = data.get("data", []) if isinstance(data, dict) else data
            return [v for v in all_votes if v.get("fixtureId") == match_id]
    except Exception as e:
        logger.warning(f"⚠️  Could not fetch voters: {e}")
    return []


def notify_all_voters(fixture: dict, title: str, body: str, ntype: str, extra_data: dict = {}):
    """Send the same notification to every voter of this fixture."""
    voters = fetch_voters(fixture["match_id"])
    if not voters:
        return
    sent = 0
    seen = set()
    for vote in voters:
        uid = vote.get("voterId", "")
        if not uid or uid in seen:
            continue
        seen.add(uid)
        send_push(uid, title, body, ntype, {
            "fixture_id": fixture["match_id"],
            "home_team":  fixture["home_team"],
            "away_team":  fixture["away_team"],
            **extra_data,
        })
        sent += 1
        time.sleep(0.05)
    logger.info(f"📲 [{ntype}] Notified {sent} voters — {fixture['home_team']} vs {fixture['away_team']}")

# ─────────────────────────────────────────────────────────────────────────────
# MATCH DAY NOTIFICATION TIMELINE
# ─────────────────────────────────────────────────────────────────────────────

# Track which pre-kickoff alerts have already been sent
# key = match_id, value = set of alert names sent
_sent_alerts: dict = {}


def _already_sent(match_id: str, alert: str) -> bool:
    return alert in _sent_alerts.get(match_id, set())


def _mark_sent(match_id: str, alert: str):
    _sent_alerts.setdefault(match_id, set()).add(alert)


def send_countdown_notifications(fixture: dict):
    """
    Check how far away kick-off is and send the appropriate countdown alert.
    Idempotent — each alert fires exactly once per fixture.
    """
    now      = datetime.now(timezone.utc)
    ko       = fixture["_kickoff_utc"]
    mins_to  = (ko - now).total_seconds() / 60
    match_id = fixture["match_id"]
    home     = fixture["home_team"]
    away     = fixture["away_team"]
    name     = f"{home} vs {away}"
    ko_eat   = (ko + NAIROBI_OFFSET).strftime("%H:%M")

    # T-60
    if 55 <= mins_to <= 65 and not _already_sent(match_id, "t60"):
        notify_all_voters(fixture,
            title=f"🔔 1 hour until kick-off!",
            body=f"{name} kicks off at {ko_eat} EAT. Pick your side!",
            ntype="kickoff_reminder_60",
            extra_data={"mins_to_kickoff": 60},
        )
        _mark_sent(match_id, "t60")

    # T-30
    elif 25 <= mins_to <= 35 and not _already_sent(match_id, "t30"):
        notify_all_voters(fixture,
            title=f"⚡ 30 minutes to go!",
            body=f"{name} — rivalries heating up. Who's winning this?",
            ntype="kickoff_reminder_30",
            extra_data={"mins_to_kickoff": 30},
        )
        _mark_sent(match_id, "t30")

    # T-10
    elif 5 <= mins_to <= 15 and not _already_sent(match_id, "t10"):
        notify_all_voters(fixture,
            title=f"🔥 10 minutes! Last chance to vote!",
            body=f"{name} — vote now before kick-off locks!",
            ntype="kickoff_reminder_10",
            extra_data={"mins_to_kickoff": 10},
        )
        _mark_sent(match_id, "t10")

    # KICK-OFF  (within 5 mins of start)
    elif -5 <= mins_to <= 5 and not _already_sent(match_id, "kickoff"):
        notify_all_voters(fixture,
            title=f"⚽ We are LIVE!",
            body=f"{home} vs {away} has kicked off. May the best pick win! 🏆",
            ntype="kickoff_live",
            extra_data={"mins_to_kickoff": 0},
        )
        _mark_sent(match_id, "kickoff")

# ─────────────────────────────────────────────────────────────────────────────
# LIVE EVENT NOTIFICATIONS
# ─────────────────────────────────────────────────────────────────────────────

def notify_goal(fixture: dict, scorer: str, new_home: int, new_away: int):
    """Personalised goal alert — different message per vote selection."""
    match_id     = fixture["match_id"]
    home_team    = fixture["home_team"]
    away_team    = fixture["away_team"]
    name         = f"{home_team} vs {away_team}"
    score_line   = f"{new_home}-{new_away}"
    scored_team  = home_team if scorer == "home_team" else away_team
    now_iso      = datetime.now(timezone.utc).isoformat()

    voters = fetch_voters(match_id)
    if not voters:
        return

    sent = 0
    seen = set()
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

    logger.info(f"📲 [goal] Notified {sent} voters — {name} {score_line}")


def notify_yellow_card(fixture: dict, team: str, score_line: str):
    name = f"{fixture['home_team']} vs {fixture['away_team']}"
    notify_all_voters(fixture,
        title=f"🟨 Yellow card — {team}",
        body=f"{name} → {score_line}. Things are heating up! 🔥",
        ntype="yellow_card",
        extra_data={"team": team, "score": score_line},
    )


def notify_corner(fixture: dict, team: str, score_line: str):
    name = f"{fixture['home_team']} vs {fixture['away_team']}"
    notify_all_voters(fixture,
        title=f"🚩 Corner to {team}",
        body=f"{name} → {score_line}. Dangerous set piece! 😤",
        ntype="corner",
        extra_data={"team": team, "score": score_line},
    )


def notify_offside(fixture: dict, team: str, score_line: str):
    name = f"{fixture['home_team']} vs {fixture['away_team']}"
    notify_all_voters(fixture,
        title=f"🚩 Offside — {team}",
        body=f"{name} → {score_line}",
        ntype="offside",
        extra_data={"team": team, "score": score_line},
    )


def notify_half_time(fixture: dict, home_score: int, away_score: int):
    name = f"{fixture['home_team']} vs {fixture['away_team']}"
    score = f"{home_score}-{away_score}"
    notify_all_voters(fixture,
        title=f"⏸ Half time — {score}",
        body=f"{name} — 45 more mins. Your pick still alive? 👀",
        ntype="half_time",
        extra_data={"home_score": home_score, "away_score": away_score},
    )


def notify_full_time(fixture: dict, home_score: int, away_score: int):
    """Full time — personalised win/loss message per voter."""
    match_id  = fixture["match_id"]
    home_team = fixture["home_team"]
    away_team = fixture["away_team"]
    name      = f"{home_team} vs {away_team}"
    score     = f"{home_score}-{away_score}"
    now_iso   = datetime.now(timezone.utc).isoformat()

    # Determine actual result
    if home_score > away_score:
        actual_result = "home_team"
    elif away_score > home_score:
        actual_result = "away_team"
    else:
        actual_result = "draw"

    voters = fetch_voters(match_id)
    if not voters:
        return

    sent = 0
    seen = set()
    for vote in voters:
        uid       = vote.get("voterId", "")
        selection = vote.get("selection", "")
        if not uid or uid in seen:
            continue
        seen.add(uid)

        if selection == actual_result:
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
            "actual_result": actual_result,
            "home_team":     home_team,
            "away_team":     away_team,
            "timestamp":     now_iso,
        })
        sent += 1
        time.sleep(0.05)

    logger.info(f"📲 [full_time] Notified {sent} voters — {name} {score}")

# ─────────────────────────────────────────────────────────────────────────────
# INCIDENT TRACKER  (yellow / corner / offside dedup)
# ─────────────────────────────────────────────────────────────────────────────

# key = match_id, value = set of incident IDs already processed
_seen_incidents: dict = {}


def process_incidents(fixture: dict, incidents: list, home_score: int, away_score: int):
    """
    Walk Sofascore incident list and fire notifications for new events.
    Sofascore incident types we care about:
      - "card"    → incidentClass "yellow"
      - "corner"
      - "offside" (not always available)
    """
    match_id  = fixture["match_id"]
    home_team = fixture["home_team"]
    away_team = fixture["away_team"]
    score_line = f"{home_score}-{away_score}"

    seen = _seen_incidents.setdefault(match_id, set())

    for inc in incidents:
        inc_id   = inc.get("id") or str(inc)
        inc_type = (inc.get("incidentType") or "").lower()
        inc_cls  = (inc.get("incidentClass") or "").lower()
        is_home  = inc.get("isHome", True)
        team     = home_team if is_home else away_team

        if inc_id in seen:
            continue
        seen.add(inc_id)

        if inc_type == "card" and inc_cls == "yellow":
            logger.info(f"🟨 Yellow card — {team}")
            notify_yellow_card(fixture, team, score_line)

        elif inc_type == "corner":
            logger.info(f"🚩 Corner — {team}")
            notify_corner(fixture, team, score_line)

        elif inc_type == "offside":
            logger.info(f"🚩 Offside — {team}")
            notify_offside(fixture, team, score_line)

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
    """
    Poll until all live fixtures finish.
    Handles: goals, yellow cards, corners, offsides, half time, full time.
    """
    watch = {f["match_id"]: f for f in live_fixtures}

    # Track half-time state per match
    half_time_sent: dict = {}

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
            score_line = f"{home_score}-{away_score}"
            status_type = live_data["status_type"]
            status_code = live_data["status_code"]
            new_status  = get_match_status(status_type, status_code)

            # ── Goal detection ─────────────────────────────────────────────
            scorer = detect_scorer(fixture, live_data)
            if scorer:
                logger.info(f"⚽ GOAL! {fixture['home_team']} vs {fixture['away_team']} → {score_line}")
                update_fixture_score(col, fixture, live_data, scorer)
                resolve_first_goal_prop(col, fixture, scorer)
                notify_goal(fixture, scorer, home_score, away_score)

                refreshed = col.find_one({"match_id": match_id})
                if refreshed:
                    refreshed["_kickoff_utc"] = fixture.get("_kickoff_utc")
                    watch[match_id] = refreshed
            else:
                update_fixture_score(col, fixture, live_data, scorer=None)

            # ── Incidents (yellow / corner / offside) ──────────────────────
            incidents = live_data.get("incidents", [])
            if incidents:
                process_incidents(fixture, incidents, home_score, away_score)

            # ── Half time ─────────────────────────────────────────────────
            # Sofascore: status_type="pause" or status_code=31
            is_half_time = (status_type == "pause" or status_code == 31)
            if is_half_time and not half_time_sent.get(match_id):
                logger.info(f"⏸  Half time — {fixture['home_team']} vs {fixture['away_team']} {score_line}")
                notify_half_time(fixture, home_score, away_score)
                half_time_sent[match_id] = True

            # Reset half-time flag when second half starts
            if status_type == "inprogress" and half_time_sent.get(match_id) is True:
                half_time_sent[match_id] = "done"  # won't reset again

            # ── Full time ──────────────────────────────────────────────────
            if new_status == "completed":
                logger.info(f"🏁 Full time — {fixture['home_team']} vs {fixture['away_team']} {score_line}")
                notify_full_time(fixture, home_score, away_score)
                watch.pop(match_id)
                continue

        if watch:
            logger.info(f"⏳ {len(watch)} game(s) live — next check in {POLL_INTERVAL_SEC}s")
            time.sleep(POLL_INTERVAL_SEC)

    logger.info("✅ All live games finished")

# ─────────────────────────────────────────────────────────────────────────────
# PRE-KICKOFF COUNTDOWN LOOP
# ─────────────────────────────────────────────────────────────────────────────

def run_countdown_for_upcoming(col, upcoming_fixtures: list):
    """
    For fixtures kicking off within the next 65 mins,
    send countdown notifications and wait for kick-off.
    """
    now = datetime.now(timezone.utc)
    soon = [
        f for f in upcoming_fixtures
        if 0 < (f["_kickoff_utc"] - now).total_seconds() / 60 <= 65
    ]

    for fixture in soon:
        send_countdown_notifications(fixture)

# ─────────────────────────────────────────────────────────────────────────────
# MAIN INFINITE LOOP
# ─────────────────────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 55)
    logger.info("⚽  FanClash Live Poller — Match Day Edition")
    logger.info("📡  Running forever — smart sleep between games")
    logger.info("=" * 55)

    mongo_client, col = connect_db()
    session = make_session()

    try:
        while True:
            try:
                all_fixtures = get_upcoming_fixtures(col)

                # 1. Send countdown notifications for upcoming games
                run_countdown_for_upcoming(col, all_fixtures)

                # 2. Poll live games
                live_now = get_live_fixtures(all_fixtures)
                if live_now:
                    logger.info(f"🔴 {len(live_now)} game(s) live — starting poller")
                    poll_live_fixtures(col, session, live_now)
                else:
                    # Nothing live — smart sleep until next event
                    smart_sleep(col)

            except KeyboardInterrupt:
                raise
            except Exception as e:
                logger.error(f"❌ Loop error: {e}", exc_info=True)
                time.sleep(60)  # brief pause before retrying

    except KeyboardInterrupt:
        logger.info("\n⏹️  Stopped by user")
    finally:
        mongo_client.close()
        logger.info("🔌 MongoDB connection closed")


if __name__ == "__main__":
    main()
