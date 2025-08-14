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
                "date": game.get("gameDate"),
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
                "season": teamrec.get("season"),
                "team_id": teamrec["team"]["id"],
                "team_name": teamrec["team"]["name"],
                "division": division_name,
                "wins": teamrec["wins"],
                "losses": teamrec["losses"],
                "win_pct": float(teamrec["winningPercentage"]),
                "games_back": float(teamrec.get("gamesBack", "0").replace("-", "0"))
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
            player_season_stats.append({
                "player_id": player.get("id"),
                "season": date.today().year,
                "team_id": p["team_id"],
                "player_name": player.get("fullName"),
                "games_played": stat.get("gamesPlayed"),
                "avg": float(stat.get("avg", 0) or 0),
                "ops": float(stat.get("ops", 0) or 0),
                "hr": int(stat.get("homeRuns", 0) or 0),
                "rbi": int(stat.get("rbi", 0) or 0),
                "era": float(stat.get("era", 0) or 0),
                "so": int(stat.get("strikeOuts", 0) or 0)
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

# --- Fetch team stats ---
def fetch_team_stats(season=None):
    if season is None:
        season = datetime.now(UTC).year

    url = f"https://statsapi.mlb.com/api/v1/teams/stats?season={season}&sportIds=1&group=hitting,pitching,fielding"
    print(f"[INFO] Fetching team stats for {season} from {url}")
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()

    team_stats = []
    for team in data.get("stats", []):
        team_info = team.get("team", {})
        stat_splits = team.get("splits", [])

        # We'll combine hitting and pitching into one row
        row = {
            "season": season,
            "team_id": team_info.get("id"),
            "team_name": team_info.get("name"),
            "games_played": None,
            "wins": None,
            "losses": None,
            "win_percentage": None,
            "runs_scored": None,
            "runs_allowed": None,
            "home_runs": None,
            "batting_avg": None,
            "obp": None,
            "slg": None,
            "era": None,
            "strikeouts": None,
            "walks": None,
            "stolen_bases": None,
            "caught_stealing": None,
            "updated_at": datetime.now(UTC).isoformat()
        }

        for split in stat_splits:
            group = split.get("group", {}).get("displayName", "").lower()
            stats = split.get("stat", {})

            if group == "hitting":
                row["games_played"] = int(stats.get("gamesPlayed", 0))
                row["runs_scored"] = int(stats.get("runs", 0))
                row["home_runs"] = int(stats.get("homeRuns", 0))
                row["batting_avg"] = float(stats.get("avg", 0) or 0)
                row["obp"] = float(stats.get("obp", 0) or 0)
                row["slg"] = float(stats.get("slg", 0) or 0)
                row["stolen_bases"] = int(stats.get("stolenBases", 0))
                row["caught_stealing"] = int(stats.get("caughtStealing", 0))

            elif group == "pitching":
                row["wins"] = int(stats.get("wins", 0))
                row["losses"] = int(stats.get("losses", 0))
                row["win_percentage"] = float(stats.get("winPercentage", 0) or 0)
                row["era"] = float(stats.get("era", 0) or 0)
                row["strikeouts"] = int(stats.get("strikeOuts", 0))
                row["walks"] = int(stats.get("baseOnBalls", 0))
                row["runs_allowed"] = int(stats.get("runs", 0))

        team_stats.append(row)

    upsert("team_stats", team_stats, ["season", "team_id"])

# --- Main runner ---
if __name__ == "__main__":
    fetch_games()
    fetch_standings()
    fetch_player_stats()
    fetch_schedule()
    fetch_team_stats()