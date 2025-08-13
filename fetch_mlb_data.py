import os
import requests
from datetime import datetime, timezone, timedelta
from supabase import create_client

# --- Supabase client setup ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

# --- Helper: Upsert into Supabase ---
def upsert(table: str, rows: list, conflict_cols: list):
    if not rows:
        print(f"[INFO] No rows to insert for table: {table}")
        return
    print(f"[INFO] Upserting {len(rows)} rows into {table}")
    response = supabase.table(table).upsert(rows, on_conflict=conflict_cols).execute()
    if hasattr(response, "data"):
        print(f"[SUCCESS] {len(response.data)} rows upserted into {table}")
    else:
        print(f"[WARNING] No response data for {table}")

# --- Fetch today's games ---
def fetch_games():
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={today}"
    r = requests.get(url)
    data = r.json()

    games = []
    for date in data.get("dates", []):
        for game in date.get("games", []):
            games.append({
                "game_id": game.get("gamePk"),
                "game_date": game.get("gameDate"),
                "status": game.get("status", {}).get("detailedState"),
                "home_team": game.get("teams", {}).get("home", {}).get("team", {}).get("name"),
                "away_team": game.get("teams", {}).get("away", {}).get("team", {}).get("name"),
                "home_score": game.get("teams", {}).get("home", {}).get("score"),
                "away_score": game.get("teams", {}).get("away", {}).get("score")
            })
    upsert("games", games, ["game_id"])

# --- Fetch team standings ---
def fetch_standings():
    url = "https://statsapi.mlb.com/api/v1/standings?leagueId=103,104"  # AL & NL
    r = requests.get(url)
    data = r.json()

    standings = []
    for record in data.get("records", []):
        for team_record in record.get("teamRecords", []):
            team = team_record.get("team", {})
            standings.append({
                "team_id": team.get("id"),
                "team_name": team.get("name"),
                "wins": team_record.get("wins"),
                "losses": team_record.get("losses"),
                "win_pct": team_record.get("winningPercentage")
            })
    upsert("standings", standings, ["team_id"])

# --- Fetch player season stats (basic batting) ---
def fetch_player_stats():
    url = "https://statsapi.mlb.com/api/v1/stats?stats=season&group=hitting&sportId=1"
    r = requests.get(url)
    data = r.json()

    player_stats = []
    for row in data.get("stats", []):
        for split in row.get("splits", []):
            player = split.get("player", {})
            stat = split.get("stat", {})
            player_stats.append({
                "player_id": player.get("id"),
                "player_name": player.get("fullName"),
                "games_played": stat.get("gamesPlayed"),
                "avg": stat.get("avg"),
                "home_runs": stat.get("homeRuns"),
                "rbi": stat.get("rbi")
            })
    upsert("player_stats", player_stats, ["player_id"])

# --- Fetch next 7 days' schedule ---
def fetch_schedule():
    start = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    end = (datetime.now(timezone.utc) + timedelta(days=7)).strftime("%Y-%m-%d")
    url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&startDate={start}&endDate={end}"
    r = requests.get(url)
    data = r.json()

    schedule = []
    for date in data.get("dates", []):
        for game in date.get("games", []):
            schedule.append({
                "game_id": game.get("gamePk"),
                "game_date": game.get("gameDate"),
                "home_team": game.get("teams", {}).get("home", {}).get("team", {}).get("name"),
                "away_team": game.get("teams", {}).get("away", {}).get("team", {}).get("name"),
            })
    upsert("schedule", schedule, ["game_id"])

# --- Main runner ---
if __name__ == "__main__":
    fetch_games()
    fetch_standings()
    fetch_player_stats()
    fetch_schedule()