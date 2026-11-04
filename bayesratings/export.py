"""Nightly data export: clean ESPN-sourced data sets as Parquet files.

    python -m bayesratings.export --db data/bayesratings.db --out data/export [--all]

Writes ``<out>/<league>/<data set>/<season>.parquet`` plus ``<out>/manifest.json``
(one entry per file: rows, columns and types, season, bytes, sha256, updated
time). Each file is written to a temp name in the same folder and renamed
into place, so a reader never sees half a file. The manifest is written the
same way, last.

What gets rebuilt each night: every season from the league's current season
on (the season in play a week ago, so the turn of a season is covered), plus
any older file that is missing. ``--all`` rebuilds everything, and so does a
change of ``VERSION`` (the manifest records the version that wrote it).

HARD RULE: only raw game, player, team and roster data. Never ratings,
forecasts, market or venue prices, blends, or any model input or internal. ``FORBIDDEN``
lists every table and column that must never be read; the SQLite authorizer
denies them at run time, so an export that tries fails instead of leaking,
and tests/test_export.py fails if any of them shows up in a file.

The database is opened read-only (``mode=ro``, ``query_only``); this module
never writes to it. All ids in the files are ESPN ids (game_id = ESPN event
id, team_id / opponent_id = ESPN team id, player_id = ESPN athlete id); our
internal team ids never leave the database.

``main`` then publishes the files to GitHub Releases
(``bayesratings.gh_publish``) when ``GITHUB_TOKEN`` is set.
The nightly job publishes to WalrusQuant/sportsleader-data.
A missing token is logged and does not fail the run. ``/data/v1`` is not
part of this step.
"""

import argparse
import bisect
import hashlib
import json
import os
import sqlite3
import sys
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

VERSION = 2
LEAGUES = ("mlb", "nba", "nfl", "nhl")
CENTRAL = ZoneInfo("America/Chicago")
REPO = Path(__file__).resolve().parent.parent
DEFAULT_OUT = REPO / "data" / "export"

# --------------------------------------------------------------- the hard rule

# Tables that hold forecasts, prices, blends or model internals. Never read.
FORBIDDEN_TABLES = frozenset({
    "forecast_snapshots", "archived_forecast_snapshots", "upcoming_forecasts",
    "futures_snapshots", "futures_runs",
    "kalshi_closes", "kalshi_quote_history", "kalshi_quotes",
    "market_dynamics_snapshots", "market_quote_history", "market_quotes",
    "market_venue_status", "game_odds",
    "player_values", "daily_runs", "rating_snapshots", "archived_rating_snapshots",
    "src_raw", "src_meta", "game_summaries", "pregame_snapshots",
})
# Columns in otherwise allowed tables that are model internals (rating
# uncertainty and home edge are fit parameters; the site shows only the
# rating and its 5-95% range), plus every raw payload column.
FORBIDDEN_COLUMNS = frozenset({
    ("rating_snapshots", "rating_sd"), ("rating_snapshots", "home_adv"),
    ("rating_snapshots", "backfilled"),
})
FORBIDDEN_COLUMN_NAMES = frozenset({"raw_json", "state_hash", "stats_json_raw"})

# Words that must never appear in an exported column name. Checked on every
# file before it is written, and again by the tests.
BANNED_NAME_PARTS = (
    "prob", "elo", "weight", "blend", "kalshi", "market", "odds", "moneyline",
    "spread", "forecast", "pred", "model", "logit", "bid", "ask", "fair",
    "edge", "adj", "player_value", "home_adv", "rating_sd", "futures", "sim",
    "venue_price", "price", "quote", "vig", "implied", "x2",
)
BANNED_NAME_TOKENS = frozenset({"ml", "mid", "value", "values", "p", "k", "x"})


def banned_column(name: str) -> bool:
    n = name.lower()
    if any(part in n for part in BANNED_NAME_PARTS) or n.endswith("_base"):
        return True
    return any(tok in BANNED_NAME_TOKENS for tok in n.split("_"))


def _authorizer(action, arg1, arg2, dbname, source):
    """Deny reads of forbidden tables/columns. arg1=table, arg2=column for READ."""
    if action == sqlite3.SQLITE_READ:
        table, col = (arg1 or "").lower(), (arg2 or "").lower()
        if table in FORBIDDEN_TABLES or (table, col) in FORBIDDEN_COLUMNS \
                or col in FORBIDDEN_COLUMN_NAMES:
            return sqlite3.SQLITE_DENY
    return sqlite3.SQLITE_OK


def connect_ro(path) -> sqlite3.Connection:
    """Read-only connection with the authorizer installed."""
    conn = sqlite3.connect(f"file:{Path(path).resolve()}?mode=ro", uri=True, timeout=30)
    conn.execute("PRAGMA query_only = ON")
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.row_factory = sqlite3.Row
    conn.set_authorizer(_authorizer)
    return conn


# ------------------------------------------------------------------ data sets

SEASON_TYPES = {0: "nba_cup_final", 1: "preseason", 2: "regular", 3: "postseason", 4: "offseason"}


@dataclass
class Dataset:
    name: str
    leagues: tuple
    kind: str                 # "game": rows carry a season; "state": season from a date
    sql: str                  # :league (and :season for "game"); state rows need _date
    cols: list                # [(name, type)] type in int|float|str|bool|date|ts
    post: object = None       # optional row -> row
    doc: str = ""
    extra_cols: dict = field(default_factory=dict)   # league -> [(name, type)]

    def columns(self, league):
        return self.cols + self.extra_cols.get(league, [])


def _team(alias, prefix):
    """ESPN team id, abbreviation and name from teams (internal id stays in)."""
    return (f"{alias}.espn_id AS {prefix}_id, {alias}.abbreviation AS {prefix}_abbr, "
            f"{alias}.name AS {prefix}_name")


GAME_CTX = [("league", "str"), ("season", "int"), ("season_type", "str"),
            ("game_id", "int"), ("game_date", "date"),
            ("team_id", "int"), ("team_abbr", "str"), ("team_name", "str"),
            ("opponent_id", "int"), ("opponent_abbr", "str"), ("opponent_name", "str"),
            ("is_home", "bool")]
PLAYER = [("player_id", "int"), ("player_name", "str"), ("position", "str")]


def _player_log(table, league, stats, position_col=True):
    """SQL + columns for one player game-log table. stats: [(src, out, type)]."""
    pos = "COALESCE(x.position, p.position)" if position_col else "p.position"
    sel = ", ".join(f"x.{src} AS {out}" for src, out, _ in stats)
    sql = (f"SELECT :league AS league, x.season, x.season_type, x.espn_event_id AS game_id, "
           f"x.game_date, {_team('t', 'team')}, {_team('o', 'opponent')}, x.is_home, "
           f"x.athlete_id AS player_id, p.name AS player_name, {pos} AS position, {sel} "
           f"FROM {table} x "
           f"LEFT JOIN teams t ON t.team_id = x.team_id "
           f"LEFT JOIN teams o ON o.team_id = x.opp_team_id "
           f"LEFT JOIN players p ON p.league = :league AND p.athlete_id = x.athlete_id "
           f"WHERE x.season = :season "
           f"ORDER BY x.game_date, x.espn_event_id, x.team_id, x.athlete_id")
    return sql, GAME_CTX + PLAYER + [(out, typ) for _, out, typ in stats]


def _logs(name, table, league, stats, doc, position_col=True):
    sql, cols = _player_log(table, league, stats, position_col)
    return Dataset(name, (league,), "game", sql, cols, doc=doc)


NBA_STATS = [("starter", "starter", "bool"), ("did_not_play", "did_not_play", "bool"),
             ("dnp_reason", "dnp_reason", "str"), ("ejected", "ejected", "bool"),
             ("minutes", "minutes", "float"), ("pts", "points", "int"),
             ("fgm", "fg_made", "int"), ("fga", "fg_att", "int"),
             ("fg3m", "fg3_made", "int"), ("fg3a", "fg3_att", "int"),
             ("ftm", "ft_made", "int"), ("fta", "ft_att", "int"),
             ("reb", "rebounds", "int"), ("oreb", "off_rebounds", "int"),
             ("dreb", "def_rebounds", "int"), ("ast", "assists", "int"),
             ("tov", "turnovers", "int"), ("stl", "steals", "int"),
             ("blk", "blocks", "int"), ("pf", "fouls", "int"),
             ("plus_minus", "plus_minus", "int")]
NHL_SKATER = [("unit", "unit", "str"), ("toi_sec", "toi_seconds", "int"),
              ("pp_toi_sec", "pp_toi_seconds", "int"), ("sh_toi_sec", "sh_toi_seconds", "int"),
              ("es_toi_sec", "es_toi_seconds", "int"), ("shifts", "shifts", "int"),
              ("g", "goals", "int"), ("a", "assists", "int"), ("shots", "shots", "int"),
              ("shots_missed", "shots_missed", "int"), ("plus_minus", "plus_minus", "int"),
              ("hits", "hits", "int"), ("blocks", "blocked_shots", "int"),
              ("takeaways", "takeaways", "int"), ("giveaways", "giveaways", "int"),
              ("fo_won", "faceoffs_won", "int"), ("fo_lost", "faceoffs_lost", "int"),
              ("pim", "penalty_minutes", "int"), ("so_goals", "shootout_goals", "int")]
NHL_GOALIE = [("starter", "starter", "bool"), ("goalie_order", "goalie_order", "int"),
              ("toi_sec", "toi_seconds", "int"), ("sa", "shots_against", "int"),
              ("ga", "goals_against", "int"), ("sv", "saves", "int"),
              ("sv_pct", "save_pct", "float"), ("es_sv", "even_strength_saves", "int"),
              ("pp_sv", "power_play_saves", "int"), ("sh_sv", "short_handed_saves", "int"),
              ("so_sa", "shootout_shots_against", "int"), ("so_sv", "shootout_saves", "int"),
              ("pim", "penalty_minutes", "int")]
MLB_BAT = [("starter", "starter", "bool"), ("bat_order", "batting_order", "int"),
           ("ab", "at_bats", "int"), ("r", "runs", "int"), ("h", "hits", "int"),
           ("rbi", "rbi", "int"), ("hr", "home_runs", "int"), ("bb", "walks", "int"),
           ("k", "strikeouts", "int"), ("pitches", "pitches_seen", "int")]
MLB_PITCH = [("starter", "starter", "bool"), ("pitch_order", "pitching_order", "int"),
             ("decision", "decision", "str"), ("outs", "outs", "int"),
             ("h", "hits_allowed", "int"), ("r", "runs_allowed", "int"),
             ("er", "earned_runs", "int"), ("bb", "walks", "int"), ("k", "strikeouts", "int"),
             ("hr", "home_runs_allowed", "int"), ("pitches", "pitches", "int"),
             ("strikes", "strikes", "int")]
NFL_PASS = [("qb_starter", "starter", "bool"), ("cmp", "completions", "int"),
            ("att", "attempts", "int"), ("yds", "yards", "int"), ("td", "touchdowns", "int"),
            ("ints", "interceptions", "int"), ("sacks", "sacks", "int"),
            ("sack_yds", "sack_yards_lost", "int"), ("qbr", "qbr", "float"),
            ("rating", "passer_rating", "float")]
NFL_RUSH = [("car", "carries", "int"), ("yds", "yards", "int"), ("td", "touchdowns", "int"),
            ("long", "longest", "int")]
NFL_REC = [("rec", "receptions", "int"), ("tgt", "targets", "int"), ("yds", "yards", "int"),
           ("td", "touchdowns", "int"), ("long", "longest", "int")]

# ---- games
GAMES_SQL = (
    "SELECT g.league, g.season, g.season_type, g.espn_event_id AS game_id, g.game_date, "
    "g.start_time_utc AS start_time, lower(replace(g.status, 'STATUS_', '')) AS status, "
    "g.completed, " + _team("h", "home_team") + ", " + _team("a", "away_team") + ", "
    "CASE WHEN g.completed = 1 THEN g.home_score END AS home_score, "
    "CASE WHEN g.completed = 1 THEN g.away_score END AS away_score, "
    "g.ending, g.neutral_site, g.venue, g.venue_city, g.venue_state, g.venue_country, "
    "g.notes, g.series_summary, g.if_necessary "
    "FROM games g LEFT JOIN teams h ON h.team_id = g.home_team_id "
    "LEFT JOIN teams a ON a.team_id = g.away_team_id "
    "WHERE g.league = :league AND g.season = :season AND g.status != 'STATUS_REMOVED' "
    "ORDER BY g.start_time_utc, g.espn_event_id")
GAMES_COLS = [("league", "str"), ("season", "int"), ("season_type", "str"), ("game_id", "int"),
              ("game_date", "date"), ("start_time", "ts"), ("status", "str"),
              ("completed", "bool"),
              ("home_team_id", "int"), ("home_team_abbr", "str"), ("home_team_name", "str"),
              ("away_team_id", "int"), ("away_team_abbr", "str"), ("away_team_name", "str"),
              ("home_score", "int"), ("away_score", "int"), ("ending", "str"),
              ("neutral_site", "bool"), ("venue", "str"), ("venue_city", "str"),
              ("venue_state", "str"), ("venue_country", "str"), ("notes", "str"),
              ("series_summary", "str"), ("if_necessary", "bool")]

# ---- team box scores (ESPN team box line)
BOX = {
    "nba": [((None, "fieldGoalPct"), "fg_pct"), ((None, "threePointFieldGoalPct"), "fg3_pct"),
            ((None, "totalRebounds"), "rebounds"), ((None, "assists"), "assists"),
            ((None, "totalTurnovers"), "turnovers"), ((None, "pointsInPaint"), "points_in_paint")],
    "nfl": [((None, "totalYards"), "total_yards"), ((None, "yardsPerPlay"), "yards_per_play"),
            ((None, "netPassingYards"), "net_passing_yards"), ((None, "rushingYards"), "rushing_yards"),
            ((None, "firstDowns"), "first_downs"), ((None, "turnovers"), "turnovers")],
    "nhl": [((None, "shotsTotal"), "shots"), ((None, "faceoffPercent"), "faceoff_pct"),
            ((None, "hits"), "hits"), ((None, "blockedShots"), "blocked_shots"),
            ((None, "takeaways"), "takeaways"), ((None, "giveaways"), "giveaways")],
    "mlb": [(("batting", "hits"), "hits"), (("batting", "homeRuns"), "home_runs"),
            (("batting", "walks"), "walks"), (("batting", "strikeouts"), "batting_strikeouts"),
            (("pitching", "strikeouts"), "pitching_strikeouts"), (("fielding", "errors"), "errors")],
}
BOX_FLOAT = {"fg_pct": "float", "fg3_pct": "float", "yards_per_play": "float",
             "faceoff_pct": "float"}
BOX_SQL = (
    "SELECT g.league, g.season, g.season_type, g.espn_event_id AS game_id, g.game_date, "
    + _team("t", "team") + ", " + _team("o", "opponent") + ", "
    "(g.home_team_id = b.team_id) AS is_home, "
    "CASE WHEN g.home_team_id = b.team_id THEN g.home_score ELSE g.away_score END AS score, "
    "CASE WHEN g.home_team_id = b.team_id THEN g.away_score ELSE g.home_score END AS opponent_score, "
    "b.stats_json AS _box "
    "FROM team_game_box b JOIN games g ON g.espn_event_id = b.espn_event_id "
    "LEFT JOIN teams t ON t.team_id = b.team_id "
    "LEFT JOIN teams o ON o.team_id = CASE WHEN g.home_team_id = b.team_id "
    "THEN g.away_team_id ELSE g.home_team_id END "
    "WHERE b.league = :league AND g.season = :season "
    "ORDER BY g.game_date, g.espn_event_id, b.team_id")


def _num(v):
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace(",", "")
    try:
        return float(s)
    except ValueError:
        return None


def _box_post(league):
    def post(r):
        raw = r.pop("_box", None)
        vals = {}
        for cat, name, val in (json.loads(raw) if raw else []):
            vals[(cat, name)] = val
        for key, out in BOX[league]:
            r[out] = _num(vals.get(key))
        return r
    return post


# ---- team game stats (Team Lab counts; for and against)
COUNT_NAMES = {
    "nfl": {"yd": "total_yards", "plays": "plays", "net_pass": "net_passing_yards",
            "cmp": "pass_completions", "att": "pass_attempts", "sacks": "sacks_taken",
            "sack_yds": "sack_yards_lost", "rush_yd": "rushing_yards", "rush_att": "rushing_attempts",
            "fd_pass": "first_downs_passing", "fd_rush": "first_downs_rushing",
            "fd_pen": "first_downs_penalty", "d3m": "third_down_conversions",
            "d3a": "third_down_attempts", "d4m": "fourth_down_conversions",
            "d4a": "fourth_down_attempts", "rz_td": "red_zone_touchdowns",
            "rz_att": "red_zone_trips", "pen": "penalties", "pen_yds": "penalty_yards",
            "to": "turnovers", "top": "possession_seconds", "drives": "drives",
            "score_dr": "scoring_drives", "def_sk": "def_sacks", "def_tfl": "def_tackles_for_loss",
            "def_pd": "def_passes_defended", "def_qbh": "def_qb_hits"},
    "nba": {"fgm": "fg_made", "fga": "fg_att", "tpm": "fg3_made", "tpa": "fg3_att",
            "ftm": "ft_made", "fta": "ft_att", "oreb": "off_rebounds", "dreb": "def_rebounds",
            "ast": "assists", "stl": "steals", "blk": "blocks", "tov": "turnovers",
            "pf": "fouls", "fbp": "fast_break_points", "pip": "points_in_paint",
            "pot": "points_off_turnovers", "poss": "possessions", "minutes": "minutes"},
    "nhl": {"sog": "shots_on_goal", "hits": "hits", "blk": "blocked_shots", "tk": "takeaways",
            "gv": "giveaways", "ppg": "power_play_goals", "ppo": "power_play_opportunities",
            "shg": "short_handed_goals", "pim": "penalty_minutes", "pen": "penalties",
            "fo": "faceoffs_won", "sa": "shots_against", "sv": "saves"},
    "mlb": {"ab": "at_bats", "h": "hits", "dbl": "doubles", "tpl": "triples", "hr": "home_runs",
            "bb": "walks", "hbp": "hit_by_pitch", "k": "strikeouts", "sb": "stolen_bases",
            "cs": "caught_stealing", "sf": "sac_flies", "tb": "total_bases",
            "pa": "plate_appearances", "lob": "left_on_base", "p_outs": "pitching_outs",
            "p_h": "hits_allowed", "p_er": "earned_runs_allowed", "p_bb": "walks_allowed",
            "p_k": "strikeouts_pitched", "p_hr": "home_runs_allowed", "p_hbp": "hit_batters",
            "qs": "quality_starts", "sv": "saves", "bs": "blown_saves",
            "bp_outs": "bullpen_outs", "bp_er": "bullpen_earned_runs", "po": "putouts",
            "a": "fielding_assists", "e": "errors", "dp": "double_plays"},
}
STATS_SQL = (
    "SELECT s.league, s.season, s.season_type, s.espn_event_id AS game_id, s.game_date, "
    + _team("t", "team") + ", " + _team("o", "opponent") + ", s.is_home, "
    "s.points_for AS score, s.points_against AS opponent_score, s.counts_json AS _counts "
    "FROM team_game_stats s LEFT JOIN teams t ON t.team_id = s.team_id "
    "LEFT JOIN teams o ON o.team_id = s.opp_team_id "
    "WHERE s.league = :league AND s.season = :season "
    "ORDER BY s.game_date, s.espn_event_id, s.team_id")


def _count_keys(league):
    from .labstats import COUNTS              # display-only counts; nothing feeds the model
    return COUNTS[league]


def _stats_post(league):
    keys = _count_keys(league)
    names = COUNT_NAMES[league]

    def post(r):
        raw = r.pop("_counts", None)
        mine, theirs = json.loads(raw) if raw else ([], [])
        for i, k in enumerate(keys):
            r[names[k]] = _num(mine[i]) if i < len(mine) else None
        for i, k in enumerate(keys):
            r["opponent_" + names[k]] = _num(theirs[i]) if i < len(theirs) else None
        return r
    return post


def _stats_cols(league):
    names = [COUNT_NAMES[league][k] for k in _count_keys(league)]
    typ = {n: ("float" if n in COUNT_FLOAT else "int") for n in names}
    return [(n, typ[n]) for n in names] + [("opponent_" + n, typ[n]) for n in names]


COUNT_FLOAT = {"possessions", "minutes"}


# ---- state tables (season comes from a date)
STATE_TAIL = [("valid_from", "ts"), ("valid_to", "ts"), ("last_seen", "ts")]
ROSTERS_SQL = (
    "SELECT r.league, r.valid_from AS _date, " + _team("t", "team") + ", "
    "r.athlete_id AS player_id, r.name AS player_name, r.position, r.jersey, "
    "r.roster_group, r.status, r.injury_status, r.injury_date, "
    "r.valid_from, r.valid_to, r.last_seen "
    "FROM roster_states r LEFT JOIN teams t ON t.team_id = r.team_id "
    "WHERE r.league = :league ORDER BY r.valid_from, r.id")
ROSTERS_COLS = [("league", "str"), ("season", "int"), ("team_id", "int"), ("team_abbr", "str"),
                ("team_name", "str"), ("player_id", "int"), ("player_name", "str"),
                ("position", "str"), ("jersey", "str"), ("roster_group", "str"),
                ("status", "str"), ("injury_status", "str"), ("injury_date", "ts")] + STATE_TAIL

INJ_COLS = [("source", "str"), ("league", "str"), ("season", "int"),
            ("team_id", "int"), ("team_abbr", "str"), ("team_name", "str"),
            ("player_id", "int"), ("player_name", "str"), ("position", "str"),
            ("status", "str"), ("injury", "str"), ("detail", "str"), ("side", "str"),
            ("return_date", "date"), ("practice_status", "str"), ("game_id", "int"),
            ("game_date", "date"), ("week", "int"), ("comment", "str"),
            ("reported_at", "ts")] + STATE_TAIL
INJ_SQL = (
    "SELECT 'espn' AS source, i.league, i.valid_from AS _date, " + _team("t", "team") + ", "
    "i.athlete_id AS player_id, p.name AS player_name, p.position, i.status, "
    "i.type AS injury, i.detail, i.side, i.return_date, NULL AS practice_status, "
    "NULL AS game_id, NULL AS game_date, NULL AS week, i.short_comment AS comment, "
    "i.injury_date AS reported_at, i.valid_from, i.valid_to, i.last_seen "
    "FROM injury_details i LEFT JOIN teams t ON t.team_id = i.team_id "
    "LEFT JOIN players p ON p.league = i.league AND p.athlete_id = i.athlete_id "
    "WHERE i.league = :league "
    "UNION ALL "
    "SELECT 'nba_official_report', n.league, COALESCE(n.game_date, n.valid_from), "
    + _team("t", "team") + ", n.athlete_id, COALESCE(p.name, n.player_name), p.position, "
    "n.status, n.reason, NULL, NULL, NULL, NULL, n.espn_event_id, n.game_date, NULL, NULL, "
    "NULL, n.valid_from, n.valid_to, n.last_seen "
    "FROM nba_injury_states n LEFT JOIN teams t ON t.team_id = n.team_id "
    "LEFT JOIN players p ON p.league = 'nba' AND p.athlete_id = n.athlete_id "
    "WHERE n.league = :league "
    "UNION ALL "
    "SELECT 'nfl_practice_report', f.league, f.valid_from, " + _team("t", "team") + ", "
    "f.athlete_id, f.full_name, f.position, f.report_status, "
    "COALESCE(f.report_primary_injury, f.practice_primary_injury), "
    "COALESCE(f.report_secondary_injury, f.practice_secondary_injury), NULL, NULL, "
    "f.practice_status, NULL, NULL, f.week, NULL, NULL, f.valid_from, f.valid_to, f.last_seen "
    "FROM nfl_injury_report_states f LEFT JOIN teams t ON t.team_id = f.team_id "
    "WHERE f.league = :league "
    "ORDER BY valid_from")

TX_COLS = [("source", "str"), ("league", "str"), ("season", "int"), ("date", "date"),
           ("team_id", "int"), ("team_abbr", "str"), ("team_name", "str"),
           ("player_id", "int"), ("player_name", "str"), ("type", "str"),
           ("description", "str"), ("first_seen", "ts")]
TX_SQL = (
    "SELECT 'espn' AS source, x.league, x.txn_date AS _date, x.txn_date AS date, "
    + _team("t", "team") + ", NULL AS player_id, NULL AS player_name, NULL AS type, "
    "x.description, x.first_seen "
    "FROM transactions x LEFT JOIN teams t ON t.team_id = x.team_id WHERE x.league = :league "
    "UNION ALL "
    "SELECT 'mlb', 'mlb', m.txn_date, m.txn_date, " + _team("t", "team") + ", m.athlete_id, "
    "p.name, m.type_desc, m.description, m.first_seen "
    "FROM mlb_transactions m LEFT JOIN teams t ON t.team_id = m.team_id "
    "LEFT JOIN players p ON p.league = 'mlb' AND p.athlete_id = m.athlete_id "
    "WHERE :league = 'mlb' "
    "ORDER BY date, first_seen")

DEPTH_COLS = [("league", "str"), ("season", "int"), ("team_id", "int"), ("team_abbr", "str"),
              ("team_name", "str"), ("formation", "str"), ("slot", "str"), ("depth", "int"),
              ("player_id", "int"), ("player_name", "str"), ("position", "str")] + STATE_TAIL
DEPTH_SQL = (
    "SELECT d.league, d.valid_from AS _date, " + _team("t", "team") + ", d.formation, d.slot, "
    "d.depth, d.athlete_id AS player_id, d.name AS player_name, d.position, "
    "d.valid_from, d.valid_to, d.last_seen "
    "FROM nfl_depth_states d LEFT JOIN teams t ON t.team_id = d.team_id "
    "WHERE d.league = :league ORDER BY d.valid_from, d.id")

PROB_COLS = [("source", "str"), ("league", "str"), ("season", "int"), ("game_id", "int"),
             ("game_date", "date"), ("team_id", "int"), ("team_abbr", "str"),
             ("team_name", "str"), ("role", "str"), ("player_id", "int"),
             ("player_name", "str"), ("status", "str"), ("note", "str")] + STATE_TAIL
PROB_SQL = (
    "SELECT 'espn' AS source, gp.league, COALESCE(g.game_date, gp.valid_from) AS _date, "
    "gp.espn_event_id AS game_id, g.game_date, " + _team("t", "team") + ", "
    "CASE gp.kind WHEN 'SP' THEN 'starting_pitcher' WHEN 'SG' THEN 'starting_goalie' "
    "ELSE lower(gp.kind) END AS role, gp.athlete_id AS player_id, gp.name AS player_name, "
    "gp.status, NULL AS note, gp.valid_from, gp.valid_to, gp.last_seen "
    "FROM game_probables gp LEFT JOIN games g ON g.espn_event_id = gp.espn_event_id "
    "LEFT JOIN teams t ON t.team_id = gp.team_id WHERE gp.league = :league "
    "UNION ALL "
    "SELECT 'daily_faceoff', s.league, s.game_date, s.espn_event_id, s.game_date, "
    + _team("t", "team") + ", 'starting_goalie', s.athlete_id, s.goalie_name, s.strength, "
    "s.news_details, s.valid_from, s.valid_to, s.last_seen "
    "FROM nhl_goalie_states s LEFT JOIN teams t ON t.team_id = s.team_id "
    "WHERE s.league = :league "
    "ORDER BY valid_from")

LINEUP_COLS = [("source", "str"), ("league", "str"), ("season", "int"), ("game_id", "int"),
               ("game_date", "date"), ("team_id", "int"), ("team_abbr", "str"),
               ("team_name", "str"), ("role", "str"), ("slot", "str"), ("player_id", "int"),
               ("player_name", "str"), ("position", "str"), ("status", "str")] + STATE_TAIL
LINEUP_SQL = (
    "SELECT 'mlb' AS source, m.league, COALESCE(g.game_date, m.valid_from) AS _date, "
    "m.espn_event_id AS game_id, g.game_date, " + _team("t", "team") + ", "
    "CASE WHEN m.slot = 0 THEN 'starting_pitcher' ELSE 'batting_order' END AS role, "
    "CAST(m.slot AS TEXT) AS slot, m.athlete_id AS player_id, m.name AS player_name, "
    "m.position, NULL AS status, m.valid_from, m.valid_to, m.last_seen "
    "FROM mlb_lineup_states m LEFT JOIN games g ON g.espn_event_id = m.espn_event_id "
    "LEFT JOIN teams t ON t.team_id = m.team_id WHERE m.league = :league "
    "UNION ALL "
    "SELECT CASE n.src WHEN 'nba.com' THEN 'nba' ELSE 'espn_depth' END, n.league, "
    "COALESCE(g.game_date, n.valid_from), n.espn_event_id, g.game_date, "
    + _team("t", "team") + ", 'starting_five', n.slot, n.athlete_id, n.name, n.slot, n.status, "
    "n.valid_from, n.valid_to, n.last_seen "
    "FROM nba_lineup_states n LEFT JOIN games g ON g.espn_event_id = n.espn_event_id "
    "LEFT JOIN teams t ON t.team_id = n.team_id WHERE n.league = :league "
    "UNION ALL "
    "SELECT 'daily_faceoff', l.league, l.valid_from, NULL, NULL, " + _team("t", "team") + ", "
    "'line_' || l.category, l.group_id, l.athlete_id, l.name, l.position_id, "
    "l.injury_status, l.valid_from, l.valid_to, l.last_seen "
    "FROM nhl_line_states l LEFT JOIN teams t ON t.team_id = l.team_id "
    "WHERE l.league = :league "
    "UNION ALL "
    "SELECT 'espn_pregame', g.league, g.game_date, pl.espn_event_id, g.game_date, "
    + _team("t", "team") + ", pl.role, CAST(pl.order_no AS TEXT), pl.athlete_id, pl.name, "
    "pl.position, pl.status, pl.fetched_at, NULL, pl.fetched_at "
    "FROM pregame_lineups pl JOIN games g ON g.espn_event_id = pl.espn_event_id "
    "LEFT JOIN teams t ON t.team_id = pl.team_id WHERE g.league = :league "
    "ORDER BY valid_from")

WX_COLS = [("source", "str"), ("league", "str"), ("season", "int"), ("game_id", "int"),
           ("game_date", "date"), ("home_team_id", "int"), ("home_team_abbr", "str"),
           ("away_team_id", "int"), ("away_team_abbr", "str"), ("venue", "str"),
           ("condition", "str"), ("temperature_f", "float"), ("high_f", "float"),
           ("low_f", "float"), ("wind_mph", "float"), ("wind_gust_mph", "float"),
           ("wind_dir", "str"), ("humidity_pct", "float"), ("precip_pct", "float"),
           ("dewpoint_f", "float"), ("fetch_phase", "str"), ("hour_offset", "int"),
           ("period_start", "ts"), ("valid_from", "ts"), ("valid_to", "ts")]
_WX_GAME = ("g.espn_event_id AS game_id, g.game_date, h.espn_id AS home_team_id, "
            "h.abbreviation AS home_team_abbr, a.espn_id AS away_team_id, "
            "a.abbreviation AS away_team_abbr, g.venue")
_WX_JOIN = ("JOIN games g ON g.espn_event_id = w.espn_event_id "
            "LEFT JOIN teams h ON h.team_id = g.home_team_id "
            "LEFT JOIN teams a ON a.team_id = g.away_team_id")
WX_SQL = (
    "SELECT 'espn_' || w.source AS source, g.league, g.game_date AS _date, " + _WX_GAME + ", "
    "w.condition, w.temperature AS temperature_f, w.high_temp AS high_f, w.low_temp AS low_f, "
    "NULL AS wind_mph, w.gust AS wind_gust_mph, NULL AS wind_dir, NULL AS humidity_pct, "
    "w.precipitation AS precip_pct, NULL AS dewpoint_f, NULL AS fetch_phase, "
    "NULL AS hour_offset, NULL AS period_start, w.valid_from, w.valid_to "
    "FROM game_weather w " + _WX_JOIN + " WHERE g.league = :league "
    "UNION ALL "
    "SELECT 'nws', g.league, g.game_date, " + _WX_GAME + ", w.short_forecast, w.temp_f, NULL, "
    "NULL, w.wind_mph, w.wind_gust_mph, w.wind_dir, w.humidity, w.precip_pct, w.dewpoint_f, "
    "w.phase, w.hour_offset, w.period_start, w.fetched_at, NULL "
    "FROM game_weather_fcst w " + _WX_JOIN + " WHERE g.league = :league "
    "ORDER BY game_date, game_id, valid_from")

def _ctx_cols(extra):
    return list(GAME_CTX) + [("score", "int"), ("opponent_score", "int")] + extra


DATASETS = [
    Dataset("games", LEAGUES, "game", GAMES_SQL, GAMES_COLS,
            doc="Schedule and results, one row per game (upcoming games have no score)."),
    Dataset("team_box_scores", LEAGUES, "game", BOX_SQL, _ctx_cols([]),
            extra_cols={lg: [(out, BOX_FLOAT.get(out, "int")) for _, out in BOX[lg]] for lg in LEAGUES},
            doc="ESPN team box line, one row per team per game."),
    Dataset("team_game_stats", LEAGUES, "game", STATS_SQL, _ctx_cols([]),
            extra_cols={lg: _stats_cols(lg) for lg in LEAGUES},
            doc="Full team counting stats for and against, one row per team per game."),
    _logs("player_game_logs", "player_game_nba", "nba", NBA_STATS,
          "NBA box line, one row per player per game."),
    _logs("skater_game_logs", "player_game_nhl_skater", "nhl", NHL_SKATER,
          "NHL skater box line, one row per player per game."),
    _logs("goalie_game_logs", "player_game_nhl_goalie", "nhl", NHL_GOALIE,
          "NHL goalie box line, one row per goalie per game."),
    _logs("batting_game_logs", "player_game_mlb_batting", "mlb", MLB_BAT,
          "MLB batting line, one row per batter per game."),
    _logs("pitching_game_logs", "player_game_mlb_pitching", "mlb", MLB_PITCH,
          "MLB pitching line, one row per pitcher per game."),
    _logs("passing_game_logs", "player_game_nfl_passing", "nfl", NFL_PASS,
          "NFL passing line, one row per passer per game.", position_col=False),
    _logs("rushing_game_logs", "player_game_nfl_rushing", "nfl", NFL_RUSH,
          "NFL rushing line, one row per rusher per game.", position_col=False),
    _logs("receiving_game_logs", "player_game_nfl_receiving", "nfl", NFL_REC,
          "NFL receiving line, one row per receiver per game.", position_col=False),
    Dataset("rosters", LEAGUES, "state", ROSTERS_SQL, ROSTERS_COLS,
            doc="ESPN roster entries with the time span each was current (valid_to empty = still current)."),
    Dataset("injuries", LEAGUES, "state", INJ_SQL, INJ_COLS,
            doc="Injury entries: ESPN (all leagues), NBA official report, NFL practice report."),
    Dataset("transactions", LEAGUES, "state", TX_SQL, TX_COLS,
            doc="Transactions: ESPN (all leagues) and MLB official."),
    Dataset("depth_charts", ("nfl",), "state", DEPTH_SQL, DEPTH_COLS,
            doc="NFL depth chart slots with the time span each was current."),
    Dataset("probables", ("mlb", "nhl"), "state", PROB_SQL, PROB_COLS,
            doc="Probable starting pitchers and goalies (ESPN, Daily Faceoff)."),
    Dataset("lineups", ("mlb", "nba", "nhl"), "state", LINEUP_SQL, LINEUP_COLS,
            doc="Announced lineups: MLB batting orders, NBA starting fives, NHL lines, ESPN pre-game."),
    Dataset("weather", ("mlb", "nfl"), "state", WX_SQL, WX_COLS,
            doc="Game weather: ESPN conditions and National Weather Service hourly outlook for the venue."),
]
for _d in DATASETS:
    if _d.name == "team_box_scores":
        _d.post = "box"
    elif _d.name == "team_game_stats":
        _d.post = "stats"

BY_NAME = {d.name: d for d in DATASETS}


def _post_fn(ds, league):
    if ds.post == "box":
        return _box_post(league)
    if ds.post == "stats":
        return _stats_post(league)
    return ds.post


# -------------------------------------------------------------- type helpers

def _ts(v):
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)):
        return datetime.fromtimestamp(v, timezone.utc)
    s = str(v).strip().replace(" ", "T", 1)
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    if len(s) == 10:
        s += "T00:00:00+00:00"
    try:
        d = datetime.fromisoformat(s)
    except ValueError:
        return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc)


def _date(v):
    if v is None or v == "":
        return None
    try:
        return date.fromisoformat(str(v)[:10])
    except ValueError:
        return None


def _conv(typ):
    def as_int(v):
        if v is None or v == "":
            return None
        try:
            return int(float(v))
        except (TypeError, ValueError):
            return None

    def as_float(v):
        if v is None or v == "":
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    def as_bool(v):
        return None if v is None else bool(int(v))

    def as_str(v):
        return None if v is None else str(v)

    return {"int": as_int, "float": as_float, "bool": as_bool, "str": as_str,
            "date": _date, "ts": _ts}[typ]


def _arrow_type(typ):
    import pyarrow as pa
    return {"int": pa.int64(), "float": pa.float64(), "bool": pa.bool_(), "str": pa.string(),
            "date": pa.date32(), "ts": pa.timestamp("us", tz="UTC")}[typ]


# ------------------------------------------------------------------- seasons

class Seasons:
    """Season of a date for one league: the latest season whose regular season
    starts no more than LEAD days later (so the off-season before opening day
    files under the coming season, once that season's schedule exists)."""
    LEAD = 60

    def __init__(self, conn, league):
        rows = conn.execute(
            "SELECT season, MIN(game_date) FROM games WHERE league = ? AND season_type = 2 "
            "GROUP BY season ORDER BY season", (league,)).fetchall()
        self.seasons = [r[0] for r in rows]
        self.starts = [date.fromisoformat(r[1]) - timedelta(days=self.LEAD) for r in rows]

    def of(self, d):
        if d is None or not self.seasons:
            return None
        i = bisect.bisect_right(self.starts, d) - 1
        return self.seasons[max(i, 0)]


def today_ct():
    return datetime.now(CENTRAL).date()


def rebuild_from(seasons: Seasons, today=None):
    """Seasons >= this are rebuilt every night (the season in play a week ago)."""
    today = today or today_ct()
    return seasons.of(today - timedelta(days=7))


# -------------------------------------------------------------------- writer

def check_columns(ds_name, cols):
    bad = [n for n, _ in cols if banned_column(n) and not _allowed_name(ds_name, n)]
    if bad:
        raise RuntimeError(f"export {ds_name}: banned column name(s) {bad}")


def _allowed_name(ds_name, n):
    """Names that trip the word list but are plain stats: the published rating
    and its range, the NFL passer rating, and MLB runners left on base."""
    return n in ALLOWED_NAMES


ALLOWED_NAMES = frozenset({"rating", "rating_low", "rating_high", "rank", "passer_rating",
                           "left_on_base", "opponent_left_on_base"})


def _rows(conn, ds, league, season=None):
    params = {"league": league}
    if season is not None:
        params["season"] = season
    cur = conn.execute(ds.sql, params)
    names = [c[0] for c in cur.description]
    post = _post_fn(ds, league)
    for r in cur:
        row = dict(zip(names, r))
        if post:
            row = post(row)
        yield row


def _table(rows, cols, season):
    import pyarrow as pa
    arrays = []
    for name, typ in cols:
        conv = _conv(typ)
        if name == "season":
            vals = [season] * len(rows)
        elif name == "season_type":
            vals = [SEASON_TYPES.get(r.get("season_type"), None if r.get("season_type") is None
                                     else str(r.get("season_type"))) for r in rows]
        else:
            vals = [conv(r.get(name)) for r in rows]
        arrays.append(pa.array(vals, type=_arrow_type(typ)))
    return pa.Table.from_arrays(arrays, schema=pa.schema(
        [pa.field(n, _arrow_type(t)) for n, t in cols]))


def _write_atomic(table, path: Path, meta: dict):
    import pyarrow.parquet as pq
    path.parent.mkdir(parents=True, exist_ok=True)
    table = table.replace_schema_metadata({"bayesratings.export": json.dumps(meta)})
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        pq.write_table(table, tmp, compression="zstd", use_dictionary=True)
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            tmp.unlink()


def _sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _write_json_atomic(obj, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        tmp.write_text(json.dumps(obj, indent=1, sort_keys=False) + "\n")
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            tmp.unlink()


def load_manifest(out: Path) -> dict:
    p = out / "manifest.json"
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text())
    except (OSError, ValueError):
        return {}


def run(db_path, out, rebuild_all=False, leagues=LEAGUES, datasets=None, today=None,
        log=print) -> dict:
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    old = load_manifest(out)
    if old.get("version") != VERSION:
        rebuild_all = True
    files = {f["path"]: f for f in old.get("files", [])}
    conn = connect_ro(db_path)
    t0 = time.monotonic()
    written = skipped = 0
    try:
        for league in leagues:
            seasons = Seasons(conn, league)
            hot = rebuild_from(seasons, today)
            for ds in DATASETS:
                if league not in ds.leagues or (datasets and ds.name not in datasets):
                    continue
                cols = ds.columns(league)
                check_columns(ds.name, cols)
                if ds.kind == "game":
                    have = [r[0] for r in conn.execute(_seasons_sql(ds), {"league": league})]
                    groups = None
                else:
                    groups = {}
                    for row in _rows(conn, ds, league):
                        s = seasons.of(_date(row.get("_date")))
                        if s is not None:
                            groups.setdefault(s, []).append(row)
                    have = sorted(groups)
                for season in have:
                    rel = f"{league}/{ds.name}/{season}.parquet"
                    path = out / rel
                    fresh = (rebuild_all or not path.exists() or rel not in files
                             or (hot is not None and season >= hot))
                    if not fresh:
                        skipped += 1
                        continue
                    rows = groups[season] if groups is not None else list(_rows(conn, ds, league, season))
                    if not rows:
                        continue
                    table = _table(rows, cols, season)
                    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
                    _write_atomic(table, path, {"league": league, "dataset": ds.name,
                                                "season": season, "version": VERSION})
                    files[rel] = {
                        "path": rel, "league": league, "dataset": ds.name, "season": season,
                        "rows": table.num_rows,
                        "columns": [{"name": n, "type": t} for n, t in cols],
                        "bytes": path.stat().st_size, "sha256": _sha256(path),
                        "updated_at": now,
                    }
                    written += 1
                    log(f"  {rel}: {table.num_rows} rows")
    finally:
        conn.close()
    # A data set that is no longer exported (team_ratings, removed Nov 2026)
    # drops out of the manifest, so it is never uploaded again.
    names = {d.name for d in DATASETS}
    files = {k: v for k, v in files.items() if (out / k).exists() and v.get("dataset") in names}
    manifest = {
        "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "ids": "game_id = ESPN event id; team_id/opponent_id = ESPN team id; player_id = ESPN athlete id",
        "datasets": {d.name: {"leagues": list(d.leagues), "about": d.doc} for d in DATASETS},
        "files": sorted(files.values(), key=lambda f: f["path"]),
    }
    _write_json_atomic(manifest, out / "manifest.json")
    log(f"export: {written} written, {skipped} unchanged, {len(files)} files, "
        f"{time.monotonic() - t0:.1f}s")
    return manifest


def _seasons_sql(ds):
    table = {"games": "games", "team_box_scores": "games",
             "team_game_stats": "team_game_stats"}.get(ds.name)
    if table is None:                                      # player logs
        table = ds.sql.split(" FROM ", 1)[1].split()[0]
        return f"SELECT DISTINCT season FROM {table} ORDER BY season"
    return f"SELECT DISTINCT season FROM {table} WHERE league = :league ORDER BY season"


def freshness(out, now=None, max_hours=26) -> tuple:
    """(level, message) for the daily check: manifest present and recent."""
    now = now or datetime.now(timezone.utc)
    m = load_manifest(Path(out))
    if not m:
        return "FAIL", f"no manifest in {out}"
    gen = _ts(m.get("generated_at"))
    if gen is None:
        return "FAIL", "manifest has no generated_at"
    age = (now - gen).total_seconds() / 3600
    n = len(m.get("files", []))
    if age > max_hours:
        return "FAIL", f"manifest {age:.1f}h old ({n} files)"
    return "OK", f"manifest {age:.1f}h old, {n} files"


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--db", default=os.environ.get("BAYESRATINGS_DB", str(REPO / "data" / "bayesratings.db")))
    ap.add_argument("--out", default=os.environ.get("BAYESRATINGS_EXPORT_DIR", str(DEFAULT_OUT)))
    ap.add_argument("--all", action="store_true", help="rebuild every season")
    ap.add_argument("--leagues", default=",".join(LEAGUES))
    ap.add_argument("--datasets", default="", help="comma list (default: all)")
    ap.add_argument("--status-file", default="")
    a = ap.parse_args(argv)
    leagues = tuple(x.strip() for x in a.leagues.split(",") if x.strip())
    dsets = {x.strip() for x in a.datasets.split(",") if x.strip()} or None
    t0 = time.monotonic()
    stamp = f"{datetime.now(CENTRAL):%Y-%m-%d %H:%M} CT"
    try:
        m = run(a.db, a.out, a.all, leagues, dsets)
    except Exception as e:                               # noqa: BLE001 - logged, exit 1
        line = f"{stamp} FAIL export: {type(e).__name__}: {e}"
        print(line, file=sys.stderr)
        _status(a.status_file, line)
        return 1
    rows = sum(f["rows"] for f in m["files"])
    size = sum(f["bytes"] for f in m["files"])
    line = (f"{stamp} OK export: {len(m['files'])} files, {rows} rows, "
            f"{size / 1e6:.1f} MB, {time.monotonic() - t0:.0f}s")
    print(line)
    _status(a.status_file, line)
    from bayesratings import gh_publish
    return gh_publish.finish(a.out, a.status_file, stamp)


def _status(path, line):
    if not path:
        return
    try:
        with open(path, "a") as f:
            f.write(line + "\n")
    except OSError:
        pass


if __name__ == "__main__":
    sys.exit(main())
