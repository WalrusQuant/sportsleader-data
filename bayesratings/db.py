# SQLite storage layer for bayesRatings.
#
# One file on disk, stdlib sqlite3, no server. The daily pipeline is the only
# writer; the model and site read. Keeps the model pure — it takes/returns
# DataFrames; this module is the only thing that knows SQL.

import sqlite3
from pathlib import Path

import pandas as pd

SCHEMA_FILE = Path(__file__).resolve().parent / "schema.sql"

# leagues we cover; team keys the model sees are stable (our team_id as string)
LEAGUES = ("mlb", "nfl", "nba", "nhl")

# team display columns updated on re-ingest (id fields are the join keys)
_TEAM_DISPLAY_COLS = ("location", "name", "abbreviation", "color",
                      "alternate_color", "logo")


def connect(path) -> sqlite3.Connection:
    """Open (creating if needed) the SQLite DB with sane pragmas."""
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA busy_timeout = 30000")  # replay runs leagues in parallel
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    """Apply the schema (idempotent — all CREATE ... IF NOT EXISTS)."""
    conn.executescript(SCHEMA_FILE.read_text())
    _migrate(conn)
    conn.commit()


# starter adjustment audit (bayesratings/starters.py); NULL on older rows
_STARTER_AUDIT = {"home_starter_id": "INTEGER", "away_starter_id": "INTEGER",
                  "home_starter_src": "TEXT", "away_starter_src": "TEXT",
                  "home_starter_rel": "REAL", "away_starter_rel": "REAL",
                  "home_prob_base": "REAL", "total_pred_base": "REAL"}

# win-chance stack audit (bayesratings/bayes_elo.py); NULL on older rows
_STACK_AUDIT = {"p_logit": "REAL", "p_elo": "REAL", "elo_weight": "REAL",
                "model_version": "TEXT"}

# player-level adjustment audit (bayesratings/player_adj.py); NULL on older rows
_PLAYER_AUDIT = {"home_player_x": "REAL", "away_player_x": "REAL",
                 "home_player_x2": "REAL", "away_player_x2": "REAL",
                 "home_player_id": "INTEGER", "away_player_id": "INTEGER",
                 "home_player_src": "TEXT", "away_player_src": "TEXT",
                 "player_prob_base": "REAL", "player_spread_base": "REAL",
                 "player_total_base": "REAL", "player_home_total_base": "REAL",
                 "player_away_total_base": "REAL", "player_inputs": "TEXT"}

# columns added after the first schema; CREATE IF NOT EXISTS won't add them to
# an existing DB file, so add any that are missing
_ADDED_COLUMNS = {
    "games": {"ending": "TEXT", "covid_2020": "INTEGER NOT NULL DEFAULT 0",
              "notes": "TEXT", "if_necessary": "INTEGER NOT NULL DEFAULT 0",
              "series_summary": "TEXT",
              "series_completed": "INTEGER NOT NULL DEFAULT 0",
              "venue_city": "TEXT", "venue_state": "TEXT", "venue_country": "TEXT"},
    "teams": {"franchise_team_id": "INTEGER REFERENCES teams (team_id)"},
    "rating_snapshots": {"backfilled": "INTEGER NOT NULL DEFAULT 0"},
    "forecast_snapshots": {"backfilled": "INTEGER NOT NULL DEFAULT 0",
                           **_STARTER_AUDIT, **_PLAYER_AUDIT, **_STACK_AUDIT},
    "upcoming_forecasts": {**_STARTER_AUDIT, **_PLAYER_AUDIT, **_STACK_AUDIT},
    # Market Dynamics margin/total lock (bayesratings/market_dynamics.py)
    "market_dynamics_snapshots": {c: "REAL" for c in (
        "margin_md", "total_md", "margin_model", "total_model", "margin_mkt", "total_mkt")}
    | {"line_venues": "TEXT"},
    # raw MLB Stats API "espn" xref, kept even when it isn't an ESPN athlete id
    "ext_ids": {"xref_id": "INTEGER"},
}


def _migrate(conn: sqlite3.Connection) -> None:
    for table, cols in _ADDED_COLUMNS.items():
        have = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
        for col, decl in cols.items():
            if col not in have:
                try:
                    conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl}")
                except sqlite3.OperationalError as e:
                    # another process (parallel replay) added it first
                    if "duplicate column" not in str(e):
                        raise
    # trend charts read one team's history; the scorecard reads by event
    conn.execute("CREATE INDEX IF NOT EXISTS idx_rating_snap_team ON "
                 "rating_snapshots (league, team_id, snapshot_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_forecast_snap_event ON "
                 "forecast_snapshots (espn_event_id)")
    if conn.execute("PRAGMA user_version").fetchone()[0] < 1:
        _central_game_dates(conn)       # one-time: Eastern -> Central game_date
        conn.execute("PRAGMA user_version = 1")


def _central_game_dates(conn: sqlite3.Connection) -> int:
    """game_date used to be the US/Eastern date; it is now America/Chicago
    (the site's clock). Only starts between 04:00 and 06:59 UTC (11pm-1am
    Central) can differ, so only those rows are checked. Idempotent."""
    from .espn import central_date
    fix = [(central_date(r[1]), r[0]) for r in conn.execute(
        "SELECT game_id, start_time_utc, game_date FROM games "
        "WHERE substr(start_time_utc, 12, 2) IN ('04', '05', '06')")
        if central_date(r[1]) != r[2]]
    if fix:
        conn.executemany("UPDATE games SET game_date = ? WHERE game_id = ?", fix)
    return len(fix)


# Games that are not going to be played as listed: never forecast, never shown
# on a slate. STATUS_REMOVED is ours: ESPN stopped listing the event (an "if
# necessary" playoff game after a clinch, or a dropped fixture). Rows are kept.
REMOVED = "STATUS_REMOVED"
HIDDEN_STATUSES = ("STATUS_POSTPONED", "STATUS_CANCELED", "STATUS_CANCELLED",
                   "STATUS_SUSPENDED", "STATUS_FORFEIT", REMOVED)
HIDDEN_SQL = ", ".join(f"'{s}'" for s in HIDDEN_STATUSES)   # constants, safe to inline


def playable_sql(alias: str = "") -> str:
    """SQL condition (no leading AND): the game is not hidden by its status."""
    p = f"{alias}." if alias else ""
    return f"COALESCE({p}status, '') NOT IN ({HIDDEN_SQL})"


# Pandemic-affected seasons (season labels per league convention): shortened
# MLB, bubble NBA/NHL finishes and 2020-21 limited crowds, empty NFL stands.
COVID_SEASONS = {"mlb": {2020}, "nfl": {2020}, "nba": {2020, 2021},
                 "nhl": {2020, 2021}}

# Relocations where ESPN issued a new team id: old espn_id -> successor
# espn_id. The old team keeps its own row; franchise_team_id links it.
FRANCHISE_SUCCESSORS = {"nhl": {24: 129764}}   # Arizona Coyotes -> Utah


def upsert_teams(conn: sqlite3.Connection, league: str, teams: list) -> dict:
    """Insert/update teams for a league. `teams` is a list of dicts with
    `espn_id` plus any display columns. Returns {espn_id: team_id}."""
    # COALESCE so a partial payload (the scoreboard lacks location/alternate
    # color) never nulls out fuller data a /teams fetch already populated
    set_clause = ", ".join(f"{c} = COALESCE(excluded.{c}, teams.{c})"
                           for c in _TEAM_DISPLAY_COLS)
    cols = ("league", "espn_id", *_TEAM_DISPLAY_COLS)
    placeholders = ", ".join("?" for _ in cols)
    sql = (f"INSERT INTO teams ({', '.join(cols)}) VALUES ({placeholders}) "
           f"ON CONFLICT (league, espn_id) DO UPDATE SET {set_clause}")
    rows = [(league, t["espn_id"], *(t.get(c) for c in _TEAM_DISPLAY_COLS))
            for t in teams]
    conn.executemany(sql, rows)
    conn.commit()
    return team_id_map(conn, league)


def link_franchises(conn: sqlite3.Connection, league: str) -> None:
    """Set franchise_team_id for known relocations with new ESPN ids."""
    ids = team_id_map(conn, league)
    for old, new in FRANCHISE_SUCCESSORS.get(league, {}).items():
        if old in ids and new in ids:
            conn.execute("UPDATE teams SET franchise_team_id = ? "
                         "WHERE team_id = ?", (ids[new], ids[old]))
    conn.commit()


def team_id_map(conn: sqlite3.Connection, league: str) -> dict:
    cur = conn.execute("SELECT espn_id, team_id FROM teams WHERE league = ?",
                       (league,))
    return {r["espn_id"]: r["team_id"] for r in cur.fetchall()}


def upsert_games(conn: sqlite3.Connection, rows: list) -> int:
    """Insert/update games. Each row is a dict with all games columns except
    game_id. Upserts on espn_event_id — results/status fill in on re-scrape."""
    if not rows:
        return 0
    cols = ("espn_event_id", "league", "season", "season_type",
            "start_time_utc", "game_date", "home_team_id", "away_team_id",
            "home_score", "away_score", "completed", "status", "venue",
            "neutral_site", "ending", "covid_2020", "notes", "if_necessary",
            "series_summary", "series_completed",
            "venue_city", "venue_state", "venue_country")
    update_cols = cols[2:6] + cols[8:]   # everything but the keys and teams
    set_clause = ", ".join(f"{c} = excluded.{c}" for c in update_cols)
    # Teams change only on a game not yet played: ESPN lists a playoff game
    # with a placeholder ("Rays/Guardians") until the series that feeds it ends.
    # Uses the stored row's completed flag, so finished games never change teams.
    set_clause += "".join(
        f", {c} = CASE WHEN games.completed = 0 THEN excluded.{c} ELSE games.{c} END"
        for c in ("home_team_id", "away_team_id"))
    placeholders = ", ".join("?" for _ in cols)
    sql = (f"INSERT INTO games ({', '.join(cols)}) VALUES ({placeholders}) "
           f"ON CONFLICT (espn_event_id) DO UPDATE SET {set_clause}")
    conn.executemany(sql, [tuple(r.get(c) for c in cols) for r in rows])
    conn.commit()
    return len(rows)


def ingest_scoreboard(conn: sqlite3.Connection, league: str, parsed: dict) -> int:
    """Write a parsed scoreboard's games into the games table, resolving teams
    against the CANONICAL teams table (which must be seeded first from the
    /teams endpoint — see upsert_teams). Games whose teams aren't canonical
    franchises are skipped and warned: this is how All-Star / exhibition games
    (NBA All-Star ingests as season.type=2 with fake team ids) get excluded
    without a special case. Returns games written."""
    id_map = team_id_map(conn, league)
    if not id_map:
        raise RuntimeError(
            f"no canonical teams for '{league}' — seed them first via "
            f"upsert_teams(conn, '{league}', parse_teams(fetch_teams('{league}')))")
    # Teams that appear in real (non-All-Star) games but are missing from the
    # current /teams list are former franchises (e.g. Arizona Coyotes): add
    # them so their history is kept rather than skipped.
    real = [g for g in parsed["games"]
            if g.get("comp_type") != "ALLSTAR" and not g.get("exhibition")]
    seen = {e for g in real for e in (g["home_espn_id"], g["away_espn_id"])}
    missing = [t for t in parsed.get("teams", [])
               if t["espn_id"] in seen and t["espn_id"] not in id_map]
    if missing:
        id_map = upsert_teams(conn, league, missing)
        link_franchises(conn, league)
    covid = COVID_SEASONS.get(league, set())
    rows = []
    skipped = []
    for g in real:
        home = id_map.get(g["home_espn_id"])
        away = id_map.get(g["away_espn_id"])
        if home is None or away is None:
            # non-franchise team (All-Star/exhibition) or unexpected id —
            # surface it, never drop silently
            skipped.append(g["espn_event_id"])
            continue
        rows.append({
            "espn_event_id": g["espn_event_id"], "league": league,
            "season": g["season"], "season_type": g["season_type"],
            "start_time_utc": g["start_time_utc"], "game_date": g["game_date"],
            "home_team_id": home, "away_team_id": away,
            "home_score": g["home_score"], "away_score": g["away_score"],
            "completed": int(g["completed"]), "status": g["status"],
            "venue": g["venue"], "neutral_site": int(g["neutral_site"]),
            "ending": g.get("ending"),
            "covid_2020": int(g["season"] in covid),
            "notes": g.get("notes"),
            "if_necessary": int(bool(g.get("if_necessary"))),
            "series_summary": g.get("series_summary"),
            "series_completed": int(bool(g.get("series_completed"))),
            "venue_city": g.get("venue_city"),
            "venue_state": g.get("venue_state"),
            "venue_country": g.get("venue_country"),
        })
    if skipped:
        import warnings
        warnings.warn(f"{league}: {len(skipped)} game(s) skipped — unresolved "
                      f"team ids (event ids: {skipped[:10]})")
    return upsert_games(conn, rows)


# A real franchise plays dozens of games a season; exhibition squads (NHL
# All-Star divisions, 4 Nations countries) appear in at most a handful of
# games in the whole history. Games involving such teams are left out of the
# model. Rows stay in the DB.
MIN_TEAM_GAMES = 5


def exhibition_team_ids(conn: sqlite3.Connection, league: str = None) -> set:
    """Team ids with fewer than MIN_TEAM_GAMES regular/postseason games in
    their league (All-Star squads, 4 Nations countries). Same rule read_games
    uses for the model."""
    q = ("SELECT t FROM (SELECT home_team_id t FROM games WHERE season_type "
         "IN (2,3) {w} UNION ALL SELECT away_team_id FROM games WHERE "
         "season_type IN (2,3) {w}) GROUP BY t HAVING COUNT(*) < ?")
    if league:
        rows = conn.execute(q.format(w="AND league = ?"),
                            (league, league, MIN_TEAM_GAMES))
    else:
        rows = conn.execute(q.format(w=""), (MIN_TEAM_GAMES,))
    return {r[0] for r in rows}


def not_exhibition_sql(ids, alias: str = "") -> str:
    """SQL condition (no leading AND) excluding games with any team in
    `ids`. Ids are ints from the DB, inlined safely."""
    if not ids:
        return "1=1"
    lst = ", ".join(str(int(i)) for i in sorted(ids))
    p = f"{alias}." if alias else ""
    return (f"{p}home_team_id NOT IN ({lst}) AND "
            f"{p}away_team_id NOT IN ({lst})")


def read_games(conn: sqlite3.Connection, league: str, seasons=None,
               season_types=(2, 3), exclude_exhibition=True) -> pd.DataFrame:
    """Return games in the model's contract (season, date, home_team,
    away_team, home_score, away_score) for a league, plus neutral and ending.
    Team columns are the stable canonical team_id as a string — never the
    abbreviation, which changes. Regular + postseason by default; preseason
    (1) excluded. `ending` is regulation / ot / shootout for NHL and empty
    otherwise; the rating fit down-weights ot and shootout on the win term."""
    q = ["SELECT season, game_date AS date, "
         "CAST(home_team_id AS TEXT) AS home_team, "
         "CAST(away_team_id AS TEXT) AS away_team, "
         "home_score, away_score, neutral_site AS neutral, ending "
         "FROM games WHERE league = ?"]
    params = [league]
    if season_types:
        q.append(f"AND season_type IN ({', '.join('?' * len(season_types))})")
        params += list(season_types)
    if seasons:
        q.append(f"AND season IN ({', '.join('?' * len(seasons))})")
        params += list(seasons)
    if exclude_exhibition:
        st = ", ".join(str(int(x)) for x in (season_types or (1, 2, 3)))
        q.append(
            "AND home_team_id NOT IN (SELECT team_id FROM exh) "
            "AND away_team_id NOT IN (SELECT team_id FROM exh)")
        q.insert(0, (
            "WITH apps AS (SELECT home_team_id AS team_id FROM games "
            f"WHERE league = ? AND season_type IN ({st}) UNION ALL "
            "SELECT away_team_id FROM games "
            f"WHERE league = ? AND season_type IN ({st})), "
            "exh AS (SELECT team_id FROM apps GROUP BY team_id "
            f"HAVING COUNT(*) < {MIN_TEAM_GAMES})"))
        params = [league, league] + params
    q.append("ORDER BY start_time_utc")
    return pd.read_sql_query(" ".join(q), conn, params=params)


def log_run(conn: sqlite3.Connection, league: str, mode: str, started_at: str,
            finished_at: str, games_written: int, status: str,
            notes: str = None) -> None:
    conn.execute(
        "INSERT INTO scraper_runs (league, mode, started_at, finished_at, "
        "games_written, status, notes) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (league, mode, started_at, finished_at, games_written, status, notes))
    conn.commit()


# ------------------------------------------------- schedule reconciliation

def _drop_forecasts(conn: sqlite3.Connection, ids, today: str) -> None:
    """Remove display forecasts for games that won't be played: every early
    forecast and game-day forecasts from `today` on. Past graded rows stay."""
    if not ids:
        return
    q = ",".join("?" * len(ids))
    try:
        conn.execute(f"DELETE FROM upcoming_forecasts WHERE espn_event_id IN ({q})",
                     list(ids))
    except sqlite3.OperationalError:     # table not created yet
        pass
    conn.execute(f"DELETE FROM forecast_snapshots WHERE espn_event_id IN ({q}) "
                 "AND snapshot_date >= ?", [*ids, today])


def _set_removed(conn: sqlite3.Connection, ids, today: str) -> None:
    if not ids:
        return
    q = ",".join("?" * len(ids))
    with conn:
        conn.execute(f"UPDATE games SET status = ? WHERE espn_event_id IN ({q})",
                     [REMOVED, *ids])
        _drop_forecasts(conn, ids, today)


def mark_removed(conn: sqlite3.Connection, league: str, espn_date: str,
                 seen_ids, today: str) -> list:
    """ESPN's scoreboard for `espn_date` (its own Eastern bucket) no longer
    lists these unplayed games: set status STATUS_REMOVED (rows are never
    deleted; a game ESPN lists again is restored by the next upsert) and drop
    their display forecasts. Returns the removed event ids."""
    from .espn import eastern_date
    seen = {int(i) for i in seen_ids}
    gone = [r[0] for r in conn.execute(
        "SELECT espn_event_id, start_time_utc FROM games WHERE league = ? "
        "AND completed = 0 AND COALESCE(status, '') != ? "
        "AND game_date BETWEEN date(?, '-1 day') AND date(?, '+1 day')",
        (league, REMOVED, espn_date, espn_date))
        if eastern_date(r[1]) == espn_date and r[0] not in seen]
    _set_removed(conn, gone, today)
    return gone


def mark_clinched(conn: sqlite3.Connection, league: str, today: str) -> list:
    """Unplayed "if necessary" playoff games whose series is already decided
    (ESPN marks the series completed on the game itself or on a finished game
    between the same two teams that season): STATUS_REMOVED, forecasts
    dropped. Returns the event ids."""
    done = conn.execute(
        "SELECT g.espn_event_id FROM games g WHERE g.league = ? AND g.completed = 0 "
        "AND g.if_necessary = 1 AND COALESCE(g.status, '') != ? AND ("
        " g.series_completed = 1 OR EXISTS (SELECT 1 FROM games f WHERE "
        " f.league = g.league AND f.season = g.season AND f.season_type = 3 "
        " AND f.completed = 1 AND f.series_completed = 1 "
        " AND f.start_time_utc < g.start_time_utc AND ("
        "  (f.home_team_id = g.home_team_id AND f.away_team_id = g.away_team_id) OR "
        "  (f.home_team_id = g.away_team_id AND f.away_team_id = g.home_team_id))))",
        (league, REMOVED)).fetchall()
    gone = [r[0] for r in done]
    _set_removed(conn, gone, today)
    return gone


def drop_hidden_forecasts(conn: sqlite3.Connection, today: str) -> int:
    """Display forecasts for unplayed games now postponed/canceled/removed."""
    ids = [r[0] for r in conn.execute(
        f"SELECT espn_event_id FROM games WHERE completed = 0 AND status IN ({HIDDEN_SQL}) "
        "AND game_date >= date(?, '-1 day')", (today,))]
    with conn:
        _drop_forecasts(conn, ids, today)
    return len(ids)
