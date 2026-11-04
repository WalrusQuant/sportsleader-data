"""Team box-score stats per game, extracted once from the stored ESPN game
summaries so the matchup page's Team Comparison averages don't have to
decompress and parse every summary of the season on each request.

    python -m bayesratings.teambox --db data/bayesratings.db [--leagues mlb,nba]

Stores, per (game, team), only the BOX_STATS values the site shows, exactly
as read from the summary (same value / displayValue choice as before), so
the averages come out identical. Re-extracts when the summary is refetched
or BOX_STATS changes. Display only: nothing here feeds the model.
"""

import argparse
import gzip
import hashlib
import json
import sys
import time
from datetime import datetime, timezone

from . import db
from .views.fmt import BOX_STATS

LEAGUES = ("mlb", "nba", "nfl", "nhl")


def version(league):
    keys = [(g, n) for g, n, *_ in BOX_STATS.get(league, [])]
    return hashlib.sha1(repr(keys).encode()).hexdigest()[:12]


def flat_stats(box_teams, espn_id, league):
    """{(group, name): raw value} of one team's BOX_STATS, read the same way
    the Team Comparison always has. Raises StopIteration / KeyError /
    TypeError when the team or the box is missing."""
    mine = next(t for t in box_teams if str(t["team"]["id"]) == str(espn_id))
    flat = {}
    for st in mine.get("statistics", []):
        if "stats" in st:
            for x in st["stats"]:
                flat[(st["name"], x["name"])] = x.get("value")
        else:
            flat[(None, st["name"])] = st.get("displayValue")
    return {k: flat.get(k) for g, n, *_ in BOX_STATS.get(league, []) for k in [(g, n)]}


def encode(stats):
    return json.dumps([[g, n, v] for (g, n), v in stats.items()])


def decode(s):
    return {(g, n): v for g, n, v in json.loads(s)}


def _pending(conn, league):
    v = version(league)
    return conn.execute(
        "SELECT g.espn_event_id, g.home_team_id, g.away_team_id, s.fetched_at "
        "FROM games g JOIN game_summaries s USING (espn_event_id) "
        "WHERE g.league = ? AND g.completed = 1 AND EXISTS (SELECT 1 FROM ("
        "  SELECT g.home_team_id AS t UNION ALL SELECT g.away_team_id) x "
        "  WHERE NOT EXISTS (SELECT 1 FROM team_game_box b WHERE b.espn_event_id = g.espn_event_id "
        "  AND b.team_id = x.t AND b.version = ? AND b.summary_fetched_at = s.fetched_at)) "
        "ORDER BY g.game_date, g.espn_event_id", (league, v)).fetchall()


def extract(conn, leagues=LEAGUES, log=print, batch=200):
    """Fill team_game_box for completed games whose rows are missing or stale."""
    res = {}
    espn = {r[0]: r[1] for r in conn.execute("SELECT team_id, espn_id FROM teams")}
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    for lg in leagues:
        t0, v = time.monotonic(), version(lg)
        todo = _pending(conn, lg)
        ok = bad = 0
        conn.commit()
        conn.execute("BEGIN IMMEDIATE")
        for n, (eid, h, a, fetched) in enumerate(todo, 1):
            gz = conn.execute("SELECT json_gz FROM game_summaries WHERE espn_event_id = ?",
                              (eid,)).fetchone()[0]
            try:
                teams = json.loads(gzip.decompress(gz))["boxscore"]["teams"]
            except (OSError, ValueError, KeyError, TypeError):
                teams = None
            for tid in (h, a):
                try:
                    stats = encode(flat_stats(teams, espn.get(tid), lg))
                    ok += 1
                except (KeyError, TypeError, StopIteration):
                    stats = None             # unreadable: the page skips it, as before
                    bad += 1
                conn.execute(
                    "INSERT OR REPLACE INTO team_game_box (espn_event_id, team_id, league, "
                    "summary_fetched_at, version, extracted_at, stats_json) VALUES (?,?,?,?,?,?,?)",
                    (eid, tid, lg, fetched, v, now, stats))
            if n % batch == 0:
                conn.commit()
                conn.execute("BEGIN IMMEDIATE")
        conn.commit()
        res[lg] = {"games": len(todo), "ok": ok, "unreadable": bad,
                   "seconds": round(time.monotonic() - t0, 1)}
        log(f"teambox {lg}: {res[lg]}")
    return res


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--db", required=True)
    ap.add_argument("--leagues", default=",".join(LEAGUES))
    a = ap.parse_args(argv)
    conn = db.connect(a.db)
    db.init_db(conn)
    extract(conn, [x for x in a.leagues.split(",") if x])
    return 0


if __name__ == "__main__":
    sys.exit(main())
