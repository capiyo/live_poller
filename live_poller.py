"""
FanClash Live Score Poller — Render Edition (Smart Sleep Version)
==================================================================
- Preserves ALL core functionality (goal detection, notifications, cron)
- Falls back to smart sleep when cron unavailable (Render)
- Zero CPU usage between checks
- Same MongoDB structure, same API calls, same push notifications
"""

import time
import logging
import subprocess
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

DATABASE_URL    = os.environ.get("DATABASE_URL",    "mongodb+srv://Capiyo:Capiyo%401010@cluster0.22lay5z.mongodb.net/clashdb?retryWrites=true&w=majority&appName=Cluster0")
FANCLASH_API    = os.environ.get("FANCLASH_API",    "https://fanclash-api.onrender.com/api")
SOFASCORE_API   = "https://api.sofascore.com/api/v1"
SOFASCORE_HOME  = "https://www.sofascore.com"

POLLER_SCRIPT_PATH = "/poller/live_poller.py"
PYTHON_PATH        = sys.executable

NAIROBI_OFFSET    = timedelta(hours=3)
POLL_INTERVAL_SEC = 60
PRE_KICKOFF_MINS  = 30
LIVE_WINDOW_MINS  = 120

# Detect if we're on Render (no cron access)
CAN_USE_CRON = os.path.exists("/usr/bin/crontab") and not os.environ.get("RENDER")

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


def get_next_kickoff(fixtures: list) -> Optional[datetime]:
    now = datetime.now(timezone.utc)
    future = [f["_kickoff_utc"] for f in fixtures if f["_kickoff_utc"] > now]
    return min(future) if future else None

# ─────────────────────────────────────────────────────────────────────────────
# CRON SELF-SCHEDULER (only if available)
# ─────────────────────────────────────────────────────────────────────────────

CRON_MARKER = "# fanclash-live-poller"

def _read_crontab() -> list:
    result = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
    if result.returncode != 0:
        return []
    return result.stdout.splitlines()

def _write_crontab(lines: list):
    new_crontab = "\n".join(lines) + "\n"
    subprocess.run(["crontab", "-"], input=new_crontab, text=True, check=True)

def reschedule_cron(run_at: datetime):
    if not CAN_USE_CRON:
        logger.info(f"📅 Would schedule for {run_at.strftime('%Y-%m-%d %H:%M')} UTC (cron unavailable, using sleep)")
        return
    lines = [l for l in _read_crontab() if CRON_MARKER not in l]
    cron_line = (
        f"{run_at.minute} {run_at.hour} "
        f"{run_at.day} {run_at.month} * "
        f"{PYTHON_PATH} {POLLER_SCRIPT_PATH} "
        f"{CRON_MARKER}"
    )
    lines.append(cron_line)
    _write_crontab(lines)
    eat = run_at + NAIROBI_OFFSET
    logger.info(f"📅 Cron rescheduled → {eat.strftime('%Y-%m-%d %H:%M')} EAT")

def remove_from_cron():
    if not CAN_USE_CRON:
        logger.info("🗑️ Cron removal skipped (cron unavailable)")
        return
    lines = [l for l in _read_crontab() if CRON_MARKER not in l]
    _write_crontab(lines)
    logger.info("🗑️ Removed from crontab")

# ─────────────────────────────────────────────────────────────────────────────
# SMART SLEEP (for Render)
# ─────────────────────────────────────────────────────────────────────────────

def smart_sleep_until_next_game(fixtures: list):
    """Sleep exactly until the next important event"""
    
    if not fixtures:
        logger.info("💤 No upcoming fixtures — sleeping 6 hours")
        time.sleep(21600)  # 6 hours
        return
    
    now = datetime.now(timezone.utc)
    next_ko = get_next_kickoff(fixtures)
    
    if not next_ko:
        logger.info("💤 No future kick-offs — sleeping 6 hours")
        time.sleep(21600)
        return
    
    # Calculate when to wake up (30 mins before kick-off)
    wake_at = next_ko - timedelta(minutes=PRE_KICKOFF_MINS)
    
    if wake_at <= now:
        # Game is imminent or already started
        logger.info(f"⚽ Game imminent — waking in 30 seconds")
        time.sleep(30)
        return
    
    # Sleep until wake time
    sleep_seconds = (wake_at - now).total_seconds()
    wake_eat = wake_at + NAIROBI_OFFSET
    
    logger.info(f"💤 Sleeping until {wake_eat.strftime('%Y-%m-%d %H:%M')} EAT ({sleep_seconds/3600:.1f} hours)")
    time.sleep(sleep_seconds)

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
            logger.warning(f"⚠️ Sofascore {resp.status_code} for event {sofascore_id}")
            return None
        event = resp.json().get("event", {})
        return {
            "home_score": (event.get("homeScore") or {}).get("current"),
            "away_score": (event.get("awayScore") or {}).get("current"),
            "status_type": (event.get("status") or {}).get("type", ""),
            "status_code": (event.get("status") or {}).get("code", 0),
        }
    except Exception as e:
        logger.warning(f"⚠️ Error fetching event {sofascore_id}: {e}")
        return None

# ─────────────────────────────────────────────────────────────────────────────
# SCORE DIFF
# ─────────────────────────────────────────────────────────────────────────────

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


def get_match_status(status_type: str, status_code: int) -> str:
    if status_type == "inprogress":
        return "live"
    if status_code in (100, 110, 120):
        return "completed"
    return "upcoming"

# ─────────────────────────────────────────────────────────────────────────────
# DATABASE UPDATES
# ─────────────────────────────────────────────────────────────────────────────

def update_fixture_score(col, fixture: dict, new_data: dict, scorer: Optional[str]):
    new_status = get_match_status(new_data["status_type"], new_data["status_code"])
    update = {
        "$set": {
            "home_score": new_data["home_score"],
            "away_score": new_data["away_score"],
            "status": new_status,
            "is_live": new_status == "live",
            "available_for_voting": new_status == "upcoming",
        }
    }
    if scorer:
        update["$push"] = {
            "goal_events": {
                "scorer": scorer,
                "home_score": new_data["home_score"],
                "away_score": new_data["away_score"],
                "timestamp": datetime.now(timezone.utc).isoformat(),
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
                "result": scorer,
                "resolved_at": datetime.now(timezone.utc).isoformat(),
                "match_id": match_id,
            }},
            upsert=True,
        )
        logger.info(f"🏆 First goal prop resolved → {scorer} ({match_id})")

# ─────────────────────────────────────────────────────────────────────────────
# PUSH NOTIFICATIONS
# ─────────────────────────────────────────────────────────────────────────────

def fetch_voters(match_id: str) -> list:
    try:
        resp = std_requests.get(f"{FANCLASH_API}/votes/votes", timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            all_votes = data.get("data", []) if isinstance(data, dict) else data
            return [v for v in all_votes if v.get("fixtureId") == match_id]
    except Exception as e:
        logger.warning(f"⚠️ Could not fetch voters for {match_id}: {e}")
    return []


def send_push(user_id: str, title: str, body: str, data: dict):
    try:
        std_requests.post(
            f"{FANCLASH_API}/notifications/send",
            json={
                "userId": user_id,
                "notificationType": data.get("type", "goal_update"),
                "title": title,
                "body": body,
                "data": data,
            },
            timeout=10,
        )
    except Exception as e:
        logger.warning(f"⚠️ Push failed for {user_id}: {e}")


def notify_goal(fixture: dict, scorer: str, new_home: int, new_away: int):
    match_id = fixture["match_id"]
    home_team = fixture["home_team"]
    away_team = fixture["away_team"]
    fixture_name = f"{home_team} vs {away_team}"
    score_line = f"{new_home}-{new_away}"
    scored_team = home_team if scorer == "home_team" else away_team
    now_iso = datetime.now(timezone.utc).isoformat()

    voters = fetch_voters(match_id)
    if not voters:
        logger.info(f"ℹ️ No voters to notify for {fixture_name}")
        return

    notified = 0
    for vote in voters:
        user_id = vote.get("voterId", "")
        selection = vote.get("selection", "")
        if not user_id:
            continue

        if selection == scorer:
            title = f"⚽ {scored_team} scored!"
            body = f"{fixture_name} → {score_line}. Your pick is winning! 😤"
            ntype = "goal_your_team"
        elif selection == "draw":
            title = f"⚽ Goal! {scored_team} score"
            body = f"{fixture_name} → {score_line}. Your draw is under pressure 😬"
            ntype = "goal_draw_pressure"
        else:
            title = f"⚔️ {scored_team} scored against you!"
            body = f"{fixture_name} → {score_line}. Rivals are coming for you! 😈"
            ntype = "goal_rival_team"

        send_push(user_id, title, body, {
            "type": ntype,
            "fixture_id": match_id,
            "scorer": scorer,
            "home_score": new_home,
            "away_score": new_away,
            "home_team": home_team,
            "away_team": away_team,
            "timestamp": now_iso,
        })
        notified += 1
        time.sleep(0.05)

    logger.info(f"📲 Notified {notified} voters — {fixture_name}")

# ─────────────────────────────────────────────────────────────────────────────
# CORE POLL LOOP (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

def poll_live_fixtures(col, session: cffi_requests.Session, live_fixtures: list):
    watch = {f["match_id"]: f for f in live_fixtures}
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

            scorer = detect_scorer(fixture, live_data)

            if scorer:
                h, a = live_data["home_score"], live_data["away_score"]
                logger.info(
                    f"⚽ GOAL! {fixture['home_team']} vs {fixture['away_team']} "
                    f"→ {h}-{a} (scorer={scorer})"
                )
                update_fixture_score(col, fixture, live_data, scorer)
                resolve_first_goal_prop(col, fixture, scorer)
                notify_goal(fixture, scorer, h, a)

                # Refresh local copy for next diff
                refreshed = col.find_one({"match_id": match_id})
                refreshed["_kickoff_utc"] = fixture["_kickoff_utc"]
                watch[match_id] = refreshed
            else:
                update_fixture_score(col, fixture, live_data, scorer=None)

            if get_match_status(live_data["status_type"], live_data["status_code"]) == "completed":
                logger.info(f"✅ {fixture['home_team']} vs {fixture['away_team']} finished")
                watch.pop(match_id)

        if watch:
            logger.info(f"⏳ {len(watch)} game(s) still live — sleeping {POLL_INTERVAL_SEC}s")
            time.sleep(POLL_INTERVAL_SEC)

    logger.info("🏁 All live games finished")

# ─────────────────────────────────────────────────────────────────────────────
# MAIN (with smart sleep fallback)
# ─────────────────────────────────────────────────────────────────────────────

def main():
    test_mode = "--test" in sys.argv
    continuous_mode = os.environ.get("CONTINUOUS_MODE") == "true" or not CAN_USE_CRON

    logger.info("=" * 55)
    logger.info(f"⚽ FanClash Live Poller {'[TEST MODE]' if test_mode else ''}")
    logger.info(f"📡 Mode: {'Continuous (smart sleep)' if continuous_mode else 'Cron-scheduled'}")
    logger.info("=" * 55)

    mongo_client, col = connect_db()
    now_utc = datetime.now(timezone.utc)

    try:
        all_fixtures = get_upcoming_fixtures(col)

        if not all_fixtures:
            logger.info("📭 No upcoming fixtures in DB")
            if not test_mode and CAN_USE_CRON:
                remove_from_cron()
            return

        # ── TEST MODE ─────────────────────────────────────────────────────
        if test_mode:
            logger.info(f"🧪 TEST MODE — {len(all_fixtures)} fixture(s) found:")
            for f in all_fixtures:
                ko = f["_kickoff_utc"] + NAIROBI_OFFSET
                logger.info(
                    f"   • {f['home_team']} vs {f['away_team']} | "
                    f"{ko.strftime('%Y-%m-%d %H:%M')} EAT | "
                    f"sofascore_id={f.get('sofascore_id')}"
                )

            target = all_fixtures[0]
            ko = target["_kickoff_utc"] + NAIROBI_OFFSET
            logger.info(f"\n🎯 Testing: {target['home_team']} vs {target['away_team']} ({ko.strftime('%Y-%m-%d %H:%M')} EAT)")

            session = make_session()
            sofascore_id = target.get("sofascore_id")
            if sofascore_id:
                logger.info(f"📡 Fetching from Sofascore (event {sofascore_id})...")
                live_data = fetch_live_score(session, sofascore_id)
                if live_data:
                    logger.info(
                        f"📊 Score: {target['home_team']} {live_data['home_score']} - "
                        f"{live_data['away_score']} {target['away_team']} "
                        f"| status={live_data['status_type']}"
                    )
                    scorer = detect_scorer(target, live_data)
                    logger.info(f"🔍 Score change detected: {scorer or 'none'}")
                else:
                    logger.warning("⚠️ No live data — game not live yet on Sofascore")

            if "--loop" in sys.argv:
                logger.info("🔁 Starting full poll loop (Ctrl+C to stop)...")
                poll_live_fixtures(col, session, [target])
            return

        # ── NORMAL MODE ───────────────────────────────────────────────────
        live_now = get_live_fixtures(all_fixtures)

        if live_now:
            logger.info(f"🔴 {len(live_now)} game(s) live — starting poller")
            session = make_session()
            poll_live_fixtures(col, session, live_now)
            all_fixtures = get_upcoming_fixtures(col)

        # ── SCHEDULING ────────────────────────────────────────────────────
        next_ko = get_next_kickoff(all_fixtures)

        if not next_ko:
            logger.info("📭 No more upcoming fixtures")
            if CAN_USE_CRON:
                remove_from_cron()
            elif continuous_mode:
                logger.info("💤 No games — sleeping 6 hours")
                time.sleep(21600)
            return

        if continuous_mode:
            # Smart sleep until next important event
            smart_sleep_until_next_game(all_fixtures)
        else:
            # Cron mode
            wake_at = next_ko - timedelta(minutes=PRE_KICKOFF_MINS)
            eat_ko = next_ko + NAIROBI_OFFSET

            if wake_at <= now_utc:
                wake_at = now_utc + timedelta(minutes=5)
                logger.info(f"⏰ Kick-off soon ({eat_ko.strftime('%H:%M')} EAT) — waking in 5 mins")
            else:
                logger.info(
                    f"📅 Next kick-off: {eat_ko.strftime('%Y-%m-%d %H:%M')} EAT — "
                    f"waking at {(wake_at + NAIROBI_OFFSET).strftime('%H:%M')} EAT"
                )
            reschedule_cron(wake_at)

    except KeyboardInterrupt:
        logger.info("\n⏹️ Stopped by user")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        mongo_client.close()
        logger.info("🔌 MongoDB connection closed")

    logger.info("✅ Done")


if __name__ == "__main__":
    main()