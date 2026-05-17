from live_poller import make_session, poll_live_game
import time

# Manually create fixture for completed game
test_fixture = {
    "match_id": "16074990",
    "sofascore_id": 16074990,
    "home_team": "Chelsea",
    "away_team": "Manchester City",
    "home_score": 0,
    "away_score": 0,
    "status": "live",  # Force it to think it's live
    "lineups_fetched": False
}

print("🔄 Starting test polling for completed game...")
print("This will fetch lineups and events from Sofascore archive\n")

session = make_session()
poll_live_game(session, test_fixture)