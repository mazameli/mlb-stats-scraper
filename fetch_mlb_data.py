import os
import requests
from datetime import datetime, timezone
from supabase import create_client, Client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

BASE_URL = "https://statsapi.mlb.com/api/v1"

def upsert(table, rows, conflict_cols):
    if not rows:
        print(f"No data for {table}")
        return
    try:
        response = supabase.table(table).upsert(rows, on_conflict=conflict_cols).execute()
        print(f"Upserted {len(rows)} rows into {table}")
    except Exception as e:
        print(f"Error upserting {table}: {e}")

# 1. Fetch today's games
def fetch_games():
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    url = f"{BASE_URL}/schedule?sportId=1&date={today}"
    r = requests.get(url)
    r.raise_for_status()
    data = r.json()
    games = []
    for date_block in data.get("dates", []):
        for g in date_block.get("games", []):
            games.append({
                "game_id": g["gamePk"],
                "date": g["gameDate"],  # ISO 8601 datetime string
                "status": g["status"]["detailedState"],
                "home_team": g["teams"]["home"]["team"]["name"],
                "away_team": g["teams"]["away"]["team"]["name"],
            })
    upsert("games", games, ["game_id"])

# 2. Fetch team standings
def fetch_standings():
    url = f"{BASE_URL}/standings?sportId=1"
    r = requests.get(url)
    r.raise_for_status()
    data = r.json()
    standings = []
    for record in data.get("records", []):
        division_name = record.get("division", {}).get("name", "")
        for team_record in record.get("teamRecords", []):
            standings.append({
                "season": int(team_record.get("season", 0)),
                "team_id": team_record["team"]["id"],
                "team_name": team_record["team"]["name"],
                "division": division_name,
                "wins": team_record.get("wins", 0),
                "losses": team_record.get("losses", 0),
                "win_pct": float(team_record.get("winningPercentage", 0)),
                "games_back": float(team_record.get("gamesBack") or 0),
            })
    upsert("standings", standings, ["season", "team_id"])

# 3. Fetch player directory (current roster for all teams)
def fetch_players():
    teams_resp = requests.get(f"{BASE_URL}/teams?sportId=1")
    teams_resp.raise_for_status()
    teams = teams_resp.json().get("teams", [])
    players = []
    for team in teams:
        team_id = team["id"]
        roster_resp = requests.get(f"{BASE_URL}/teams/{team_id}/roster")
        roster_resp.raise_for_status()
        roster = roster_resp.json().get("roster", [])
        for player in roster:
            players.append({
                "player_id": player["person"]["id"],
                "full_name": player["person"]["fullName"],
                "position": player["position"]["abbreviation"],
                "team_id": team_id,
                "active": True,
            })
    upsert("players", players, ["player_id"])

# 4. Fetch player season stats (batting only)
def fetch_player_season_stats():
    players_resp = supabase.table("players").select("player_id,team_id").execute()
    players = players_resp.data or []
    stats = []
    season_year = datetime.now(timezone.utc).year

    for p in players:
        pid = p["player_id"]
        url = f"{BASE_URL}/people/{pid}/stats"
        params = {
            "stats": "season",
            "season": season_year,
            "group": "hitting"  # batting stats only
        }
        r = requests.get(url, params=params)
        if r.status_code != 200:
            print(f"Warning: failed to fetch stats for player {pid}")
            continue
        j = r.json()
        splits = j.get("stats", [])
        if not splits or not splits[0].get("splits"):
            continue
        stat = splits[0]["splits"][0]["stat"]
        stats.append({
            "season": season_year,
            "player_id": pid,
            "team_id": p["team_id"],
            "avg": float(stat.get("avg") or 0),
            "ops": float(stat.get("ops") or 0),
            "hr": int(stat.get("homeRuns") or 0),
            "rbi": int(stat.get("rbi") or 0),
            "so": int(stat.get("strikeOuts") or 0),
        })
    upsert("player_season_stats", stats, ["season", "player_id"])

# 5. Fetch upcoming schedule (next 7 days)
def fetch_schedule():
    from datetime import timedelta

    start_date = datetime.now(timezone.utc)
    end_date = start_date + timedelta(days=7)
    url = f"{BASE_URL}/schedule?sportId=1&startDate={start_date.date()}&endDate={end_date.date()}"
    r = requests.get(url)
    r.raise_for_status()
    data = r.json()
    schedule = []
    for date_block in data.get("dates", []):
        for g in date_block.get("games", []):
            schedule.append({
                "game_id": g["gamePk"],
                "date": g["gameDate"].split("T")[0],  # date only
                "home_team": g["teams"]["home"]["team"]["name"],
                "away_team": g["teams"]["away"]["team"]["name"],
                "venue": g.get("venue", {}).get("name", ""),
                "game_time": g["gameDate"],
                "probable_pitcher_home": g["teams"]["home"].get("probablePitcher", {}).get("fullName"),
                "probable_pitcher_away": g["teams"]["away"].get("probablePitcher", {}).get("fullName"),
            })
    upsert("schedule", schedule, ["game_id"])

if __name__ == "__main__":
    fetch_games()
    fetch_standings()
    fetch_players()
    fetch_player_season_stats()
    fetch_schedule()