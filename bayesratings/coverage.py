"""Blank stats ESPN filled with zero before it tracked them, and describe one season.

A final pro game that ESPN reports as 0-0 with no team or player box is not a
real result (event 191101009 is Seattle 27, Green Bay 7, stored as 0-0). On a
historical season the scores are cleared and the status becomes
``final_no_data``. The games columns do not change. Current seasons are left
as ESPN sent them.

A column is untracked for a season when every value is missing or zero
(plus/minus included: a real season has negatives). The column is then
written as null. A zero next to any other value is a real zero and stays.

This runs only for pro seasons older than the files that were already
published (MLB and NFL from 2021, NBA and NHL from 2022). A nightly rebuild
of a current file is left alone.

The one-season job writes ``coverage/<league>/<season>.json`` and points at
it from ``manifest["coverage"]``.
"""

# First season that was already on the manifest before history backfill.
# Blanking starts strictly below these years.
HISTORY_BEFORE = {
    "mlb": 2021,
    "nfl": 2021,
    "nba": 2022,
    "nhl": 2022,
}

# Counting columns that are results, not the box line. A shutout is a real 0.
_KEEP = {
    "season", "game_id", "home_score", "away_score", "score", "opponent_score",
}


def box_has_data(summary) -> bool:
    """True when a summary has a player line or a team statistic.

    Old finals sometimes return the category shells (passing, rushing) with
    no athletes, and a team object whose statistics list is empty. That is
    not a box."""
    if not isinstance(summary, dict):
        return False
    box = summary.get("boxscore") or {}
    for side in box.get("players") or []:
        for group in side.get("statistics") or []:
            for athlete in group.get("athletes") or []:
                if (athlete.get("athlete") or {}).get("id") not in (None, ""):
                    return True
    for side in box.get("teams") or []:
        for stat in side.get("statistics") or []:
            if stat.get("stats"):
                return True
            if stat.get("name") and (
                    stat.get("displayValue") not in (None, "")
                    or stat.get("value") not in (None, "")):
                return True
    return False


def clear_scoreless_finals(conn, league, season) -> int:
    """Null the score of a historical final that has no box, and drop derived rows.

    A 0-0 final is not a possible result in MLB, NBA, NFL, or NHL. ESPN still
    marks some old games STATUS_FINAL with both scores 0 and an empty box.
    ``completed`` stays 1. ``status`` becomes ``final_no_data``. Team box and
    team counting rows for that game are removed so they are not published.
    A current season is not touched, and a 0-0 that actually has a box is kept."""
    from . import db
    if not blanks_history(league, season):
        return 0
    hidden = db.HIDDEN_STATUSES
    rows = conn.execute(
        "SELECT g.espn_event_id, s.json_gz FROM games g "
        "LEFT JOIN game_summaries s ON s.espn_event_id = g.espn_event_id "
        "WHERE g.league = ? AND g.season = ? AND g.completed = 1 "
        "AND g.season_type IN (2, 3) AND g.home_score = 0 AND g.away_score = 0 "
        "AND COALESCE(g.status, '') NOT IN ({})".format(
            ",".join("?" * len(hidden))),
        (league, int(season), *hidden)).fetchall()
    import gzip
    import json
    clear = []
    for eid, blob in rows:
        summary = None
        if blob:
            try:
                summary = json.loads(gzip.decompress(blob))
            except (OSError, ValueError):
                summary = None
        if summary is not None and box_has_data(summary):
            continue
        clear.append(int(eid))
    if not clear:
        return 0
    marks = ",".join("?" * len(clear))
    conn.execute(
        f"UPDATE games SET home_score = NULL, away_score = NULL, status = ? "
        f"WHERE espn_event_id IN ({marks})",
        (db.FINAL_NO_DATA, *clear))
    for table in ("team_game_stats", "team_game_box"):
        conn.execute(
            f"DELETE FROM {table} WHERE espn_event_id IN ({marks})", clear)
    conn.commit()
    return len(clear)


def blanks_history(league, season) -> bool:
    floor = HISTORY_BEFORE.get(league)
    return floor is not None and int(season) < floor


def stat_column(name: str) -> bool:
    if name in _KEEP or name.endswith("_id"):
        return False
    return True


def _missing_or_zero(value) -> bool:
    if value is None or isinstance(value, bool):
        return value is None
    if isinstance(value, (int, float)):
        return value == 0
    return False


def blank_untracked(rows, columns):
    """Return (rows, blanked column names). `columns` are the stat names to scan.

    Rows are copied when a column is blanked. Other columns stay as they were."""
    names = [name for name in columns if stat_column(name)]
    if not rows or not names:
        return rows, []
    blanked = []
    for name in names:
        if all(_missing_or_zero(row.get(name)) for row in rows):
            blanked.append(name)
    if not blanked:
        return rows, []
    out = []
    for row in rows:
        copied = dict(row)
        for name in blanked:
            copied[name] = None
        out.append(copied)
    return out, blanked


def _games(conn, league, season):
    from . import db
    return conn.execute(
        "SELECT espn_event_id, season_type, completed, status FROM games "
        "WHERE league = ? AND season = ? AND season_type IN (2, 3)",
        (league, int(season))).fetchall()


def build_report(conn, league, season, blanked) -> dict:
    """One season: how many games had a player box, which stats were blanked,
    and the names ESPN used for the teams."""
    from . import db
    rows = _games(conn, league, season)
    playable = [r for r in rows
                if r["completed"] and (r["status"] or "") not in db.HIDDEN_STATUSES]
    ids = [r["espn_event_id"] for r in playable]
    with_box = 0
    if ids:
        q = ",".join("?" * len(ids))
        with_box = conn.execute(
            f"SELECT COUNT(*) FROM player_extract_log "
            f"WHERE espn_event_id IN ({q}) AND status = 'ok' AND rows > 0",
            ids).fetchone()[0]
    games = len(playable)
    coverage = round(with_box / games, 4) if games else None
    roster = []
    for row in conn.execute(
            "SELECT t.espn_id, ts.abbreviation, ts.name, "
            "(SELECT COUNT(*) FROM games g WHERE g.league = ? AND g.season = ? "
            " AND g.season_type IN (2, 3) AND g.completed = 1 "
            " AND (g.home_team_id = ts.team_id OR g.away_team_id = ts.team_id)) AS games "
            "FROM team_seasons ts JOIN teams t ON t.team_id = ts.team_id "
            "WHERE ts.league = ? AND ts.season = ? ORDER BY ts.abbreviation",
            (league, int(season), league, int(season))):
        roster.append({"team_id": row[0], "abbreviation": row[1],
                       "name": row[2], "games": row[3]})
    return {
        "league": league,
        "season": int(season),
        "games": games,
        "games_with_player_box": with_box,
        "games_missing_score": sum(
            1 for r in playable if (r["status"] or "") == db.FINAL_NO_DATA),
        "player_box_coverage": coverage,
        "flagged_below_95": bool(games) and (coverage or 0) < 0.95,
        "blanked_stats": {k: list(v) for k, v in sorted((blanked or {}).items())},
        "teams": roster,
    }
