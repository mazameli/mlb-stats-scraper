import requests
from datetime import date
from supabase import create_client, Client
import os

# Supabase setup
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Today's date
today = date.today().isoformat()

# Fetch today's games
resp = requests.get(f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={today}")
games = resp.json().get("dates", [{}])[0].get("games", [])

# Insert into Supabase
for game in games:
    data = {
        "game_id": game["gamePk"],
        "date": today,
        "home_team": game["teams"]["home"]["team"]["name"],
        "away_team": game["teams"]["away"]["team"]["name"],
        "status": game["status"]["detailedState"]
    }
    supabase.table("games").upsert(data, on_conflict=["game_id"]).execute()
