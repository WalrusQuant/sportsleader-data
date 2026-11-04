"""Four-league ESPN backfill: scoreboards day by day, plus the raw game summary
for every completed game.

    python -m bayesratings.backfill --leagues mlb,nba,nfl,nhl --seasons 5 \
        --db data/bayesratings.db

Resumable: each (league, date) is recorded in backfill_dates and each summary in
game_summaries, so a rerun skips finished work. Dates within the last two days
are always refetched (results may still be filling in). Requests are paced at
--rate seconds apart; espn._get backs off exponentially on 429/5xx.
Preseason (season.type 1) and All-Star games are not stored.
"""

import argparse
import gzip
import json
import sys
import time
from datetime import date, datetime, timedelta, timezone

from . import db
from . import espn
from . import prelog

KEEP_SEASON_TYPES = (2, 3)  # regular + postseason
RECHECK_DAYS = 2


def season_window(league: str, season: int) -> tuple:
    """Generous (start, end) date window covering a season. Events self-identify
    their season via event.season.year, so edge overlap is harmless. Windows
    follow each league's season-year convention (docs/espn-nfl-nba-nhl-notes.md)."""
    if league == "mlb":                       # calendar year; spring->Nov
        return date(season, 3, 1), date(season, 11, 30)
    if league == "nfl":                       # start year; Sep->Feb next year
        return date(season, 8, 1), date(season + 1, 2, 28)
    if league in ("nba", "nhl"):              # end year; Oct prev->Jul
        return date(season - 1, 9, 15), date(season, 7, 15)
    raise ValueError(f"unknown league {league!r}")


def current_season(league: str, today: date) -> int:
    """Season label in progress (or most recently started) on `today`."""
    if league == "mlb":
        return today.year if today.month >= 3 else today.year - 1
    if league == "nfl":
        return today.year if today.month >= 8 else today.year - 1
    if league in ("nba", "nhl"):
        return today.year + 1 if (today.month, today.day) >= (9, 15) else today.year
    raise ValueError(f"unknown league {league!r}")


def seasons_for(league: str, n_completed: int, today: date) -> list:
    """The current season plus the n_completed seasons before it."""
    cur = current_season(league, today)
    return list(range(cur - n_completed, cur + 1))


def league_dates(league: str, seasons, today: date) -> list:
    out = set()
    for s in seasons:
        start, end = season_window(league, s)
        d = start
        while d <= min(end, today):
            out.add(d)
            d += timedelta(days=1)
    return sorted(out)


class Pacer:
    """Keeps requests at least `rate` seconds apart and measures them."""

    def __init__(self, rate: float):
        self.rate = rate
        self.last = 0.0
        self.n = 0
        self.t0 = time.monotonic()

    def __call__(self, fn, *a, **kw):
        wait = self.rate - (time.monotonic() - self.last)
        if wait > 0:
            time.sleep(wait)
        self.last = time.monotonic()
        try:
            return fn(*a, **kw)
        finally:
            self.n += 1

    def per_request(self) -> float:
        return (time.monotonic() - self.t0) / max(self.n, 1)


def _done_dates(conn, league: str, recheck_from: date) -> set:
    rows = conn.execute("SELECT date FROM backfill_dates WHERE league = ? "
                        "AND status = 'done' AND date < ?",
                        (league, recheck_from.isoformat()))
    return {r[0] for r in rows}


def _missing_summaries(conn, league: str, event_ids) -> list:
    if not event_ids:
        return []
    q = ("SELECT g.espn_event_id FROM games g LEFT JOIN game_summaries s "
         "USING (espn_event_id) WHERE g.league = ? AND g.completed = 1 "
         "AND s.espn_event_id IS NULL AND g.espn_event_id IN "
         f"({', '.join('?' * len(event_ids))})")
    return [r[0] for r in conn.execute(q, (league, *event_ids))]


def store_summary(conn, league: str, event_id: int, data: dict) -> None:
    raw = json.dumps(data, separators=(",", ":")).encode()
    conn.execute(
        "INSERT INTO game_summaries (espn_event_id, league, fetched_at, "
        "raw_bytes, json_gz) VALUES (?, ?, ?, ?, ?) ON CONFLICT (espn_event_id) "
        "DO UPDATE SET fetched_at = excluded.fetched_at, "
        "raw_bytes = excluded.raw_bytes, json_gz = excluded.json_gz",
        (event_id, league, _now(), len(raw), gzip.compress(raw)))


def load_summary(conn, event_id: int) -> dict:
    row = conn.execute("SELECT json_gz FROM game_summaries WHERE "
                       "espn_event_id = ?", (event_id,)).fetchone()
    return json.loads(gzip.decompress(row[0])) if row else None


def refresh_team_seasons(conn, league: str) -> None:
    """Rebuild team_seasons: each team's most-used non-neutral home venue per
    season, plus its display name as last seen."""
    conn.execute("DELETE FROM team_seasons WHERE league = ?", (league,))
    conn.execute("""
        INSERT INTO team_seasons (team_id, league, season, name, abbreviation,
                                  home_venue, home_games)
        SELECT v.home_team_id, v.league, v.season, t.name, t.abbreviation,
               v.venue, v.n
        FROM (SELECT home_team_id, league, season, venue, COUNT(*) n,
                     ROW_NUMBER() OVER (PARTITION BY home_team_id, season
                                        ORDER BY COUNT(*) DESC) rk
              FROM games WHERE league = ? AND neutral_site = 0
                AND season_type = 2 AND venue IS NOT NULL
              GROUP BY home_team_id, season, venue) v
        JOIN teams t ON t.team_id = v.home_team_id
        WHERE v.rk = 1""", (league,))
    conn.commit()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _fmt(sec: float) -> str:
    sec = int(sec)
    return f"{sec // 3600}h{sec % 3600 // 60:02d}m"


def log(msg: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


def reconcile_date(conn, league, d, data, today) -> list:
    """Mark stored unplayed games ESPN no longer lists on scoreboard date `d`
    as STATUS_REMOVED. Only a well-formed scoreboard is trusted: it must carry
    both `events` and `leagues` (an error or partial body has neither), and
    every event must have an id. An empty but well-formed day is real (e.g.
    the day after a series clinched) and is reconciled."""
    if not isinstance(data, dict) or "events" not in data or "leagues" not in data:
        log(f"{league} {d} scoreboard malformed; not reconciling")
        return []
    ids = []
    for ev in data["events"]:
        try:
            ids.append(int(ev["id"]))
        except (KeyError, TypeError, ValueError):
            return []           # can't trust a partial read
    gone = db.mark_removed(conn, league, d.isoformat(), ids, today.isoformat())
    if gone:
        log(f"{league} {d} no longer listed by ESPN -> {db.REMOVED}: {gone}")
    return gone


def plan(conn, leagues, n_seasons, today, only_dates=None) -> dict:
    recheck_from = today - timedelta(days=RECHECK_DAYS)
    out = {}
    for lg in leagues:
        if only_dates:
            dates = only_dates
        else:
            dates = league_dates(lg, seasons_for(lg, n_seasons, today), today)
        done = _done_dates(conn, lg, recheck_from)
        out[lg] = [d for d in dates if d.isoformat() not in done]
    return out


def run(conn, leagues, n_seasons, rate=1.0, today=None, only_dates=None,
        est_games_per_date=None) -> dict:
    today = today or date.today()
    pace = Pacer(rate)
    todo = plan(conn, leagues, n_seasons, today, only_dates)
    total_dates = sum(len(v) for v in todo.values())
    # rough per-date game estimate for the ETA until real counts accumulate
    est = est_games_per_date or {"mlb": 9, "nba": 4.5, "nfl": 1.4, "nhl": 5.5}
    log(f"plan: {total_dates} dates to fetch "
        + ", ".join(f"{lg}={len(v)}" for lg, v in todo.items()))
    stats = {"dates": 0, "games": 0, "summaries": 0, "errors": 0, "removed": 0}
    dates_left = total_dates

    for lg in leagues:
        started = _now()
        if not db.team_id_map(conn, lg):
            db.upsert_teams(conn, lg, espn.parse_teams(pace(espn.fetch_teams, lg)))
        db.link_franchises(conn, lg)
        lg_games = 0
        plog = {}
        for i, d in enumerate(todo[lg]):
            ds = d.isoformat()
            try:
                data = pace(espn.fetch_scoreboard, lg, dates=f"{d:%Y%m%d}")
                parsed = espn.parse_scoreboard(data, lg)
                parsed["games"] = [g for g in parsed["games"]
                                   if g["season_type"] in KEEP_SEASON_TYPES]
                n = db.ingest_scoreboard(conn, lg, parsed)
                if d >= today - timedelta(days=1):
                    gone = reconcile_date(conn, lg, d, data, today)
                    stats["removed"] += len(gone)
                    # probables / odds / weather from the same payload (logging only)
                    prelog._add(plog, prelog.log_scoreboard(conn, lg, data, log=log))
                ids = [g["espn_event_id"] for g in parsed["games"]]
                for eid in _missing_summaries(conn, lg, ids):
                    store_summary(conn, lg, eid,
                                  pace(espn.fetch_summary, lg, eid))
                    conn.commit()
                    stats["summaries"] += 1
                # done only once every completed game has its summary
                conn.execute(
                    "INSERT OR REPLACE INTO backfill_dates VALUES (?,?,?,?,?)",
                    (lg, ds, n, "done", _now()))
                conn.commit()
                lg_games += n
                stats["games"] += n
            except Exception as e:  # keep going; date stays pending for rerun
                stats["errors"] += 1
                conn.execute(
                    "INSERT OR REPLACE INTO backfill_dates VALUES (?,?,?,?,?)",
                    (lg, ds, None, "error", _now()))
                conn.commit()
                log(f"{lg} {ds} ERROR {type(e).__name__}: {e}")
            stats["dates"] += 1
            dates_left -= 1
            if (i + 1) % 25 == 0 or i + 1 == len(todo[lg]):
                rem = sum(len(todo[x]) * (1 + est[x]) for x in
                          leagues[leagues.index(lg) + 1:])
                rem += (len(todo[lg]) - i - 1) * (1 + est[lg])
                log(f"{lg} {ds}  {i + 1}/{len(todo[lg])} dates, "
                    f"{lg_games} games, {pace.n} requests, "
                    f"{pace.per_request():.2f}s/req, "
                    f"ETA ~{_fmt(rem * pace.per_request())}")
        if plog:
            prelog.record_scoreboard_run(conn, lg, started, plog, log=log)
        stats["removed"] += len(db.mark_clinched(conn, lg, today.isoformat()))
        db.drop_hidden_forecasts(conn, today.isoformat())
        refresh_team_seasons(conn, lg)
        db.log_run(conn, lg, "backfill", started, _now(), lg_games,
                   "ok" if not stats["errors"] else "partial",
                   f"{len(todo[lg])} dates, seasons={n_seasons}+current")
    stats["requests"] = pace.n
    stats["sec_per_request"] = round(pace.per_request(), 3)
    return stats


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--leagues", default="mlb,nba,nfl,nhl")
    ap.add_argument("--seasons", type=int, default=5,
                    help="completed seasons before the current one")
    ap.add_argument("--db", default="data/bayesratings.db")
    ap.add_argument("--rate", type=float, default=1.0,
                    help="minimum seconds between requests")
    ap.add_argument("--dates", default=None,
                    help="comma list of YYYY-MM-DD; overrides the season "
                         "windows (for sampling/testing)")
    args = ap.parse_args(argv)
    leagues = [x.strip().lower() for x in args.leagues.split(",") if x.strip()]
    for lg in leagues:
        if lg not in espn.LEAGUES:
            sys.exit(f"unknown league {lg!r}")
    only = ([date.fromisoformat(x) for x in args.dates.split(",")]
            if args.dates else None)
    conn = db.connect(args.db)
    db.init_db(conn)
    stats = run(conn, leagues, args.seasons, rate=args.rate, only_dates=only)
    log(f"finished: {stats}")


if __name__ == "__main__":
    main()
