"""ESPN backfill: scoreboards day by day, plus the raw game summary for every
completed game that is new or whose score changed.

    python -m bayesratings.backfill --leagues mlb,nba,nfl,nhl --seasons 5 \
        --db data/bayesratings.db

Resumable: each (league, date) is recorded in backfill_dates and each summary in
game_summaries, so a rerun skips finished work. Dates within the last two days
are always refetched (results may still be filling in). Scoreboard requests are
paced at --rate seconds apart; espn._get backs off exponentially on 429/5xx.
Summary requests use a small pool (the nightly job and the one-season backfill
use 4) and are skipped when the game is already final with the same score.
Preseason (season.type 1) and All-Star games are not stored.

College football (`cfb`) is FBS, season = start year, from 2021.
Men's college basketball (`cbb`) is Division I, season = end year, from 2021
(the 2020-21 season).

Pro history, verified against the scoreboard: NFL from 1999 (1998 has no
games), NBA from 1994 (the 1993-94 season; 1992-93 has no player box), NHL
from 1994 (1993-94; 1992-93 has no games), MLB from 2003 (2002 has games and
no player lines). The one-season job is what publishes those years.
"""

import argparse
import gzip
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone

from . import db
from . import espn
from . import prelog

KEEP_SEASON_TYPES = (2, 3)  # regular + postseason
RECHECK_DAYS = 2
# How many summary requests may be in flight at once. The nightly job and the
# one-season backfill use this. The plain CLI stays sequential unless asked.
SUMMARY_WORKERS = 4
# First season a one-season backfill may publish. cbb 2021 is the 2020-21
# season. NBA 1994 is 1993-94. NHL 1994 is 1993-94. The nightly job does not
# read this; it only refreshes a season that is already in the manifest.
FIRST_SEASON = {
    "mlb": 2003, "nfl": 1999, "nba": 1994, "nhl": 1994,
    "cfb": 2021, "cbb": 2021,
}


def season_window(league: str, season: int) -> tuple:
    """Generous (start, end) date window covering a season. Events self-identify
    their season via event.season.year, so edge overlap is harmless. Windows
    follow each league's season-year convention (docs/espn-nfl-nba-nhl-notes.md)."""
    if league == "mlb":                       # calendar year; spring->Nov
        return date(season, 3, 1), date(season, 11, 30)
    if league == "nfl":                       # start year; Sep->Feb next year
        return date(season, 8, 1), date(season + 1, 2, 28)
    if league == "cfb":                       # start year; late August through January
        return date(season, 8, 1), date(season + 1, 2, 20)
    if league in ("nba", "nhl"):              # end year; Oct prev->Jul
        return date(season - 1, 9, 15), date(season, 7, 15)
    if league == "cbb":                       # end year; early November through early April
        return date(season - 1, 10, 15), date(season, 4, 30)
    raise ValueError(f"unknown league {league!r}")


def current_season(league: str, today: date) -> int:
    """Season label in progress (or most recently started) on `today`."""
    if league == "mlb":
        return today.year if today.month >= 3 else today.year - 1
    if league in ("nfl", "cfb"):
        return today.year if today.month >= 8 else today.year - 1
    if league in ("nba", "nhl"):
        return today.year + 1 if (today.month, today.day) >= (9, 15) else today.year
    if league == "cbb":
        # 2026-27 opened on Nov 2. From Oct 15 the coming season is current so
        # a one-season backfill can include opening week.
        return today.year + 1 if (today.month, today.day) >= (10, 15) else today.year
    raise ValueError(f"unknown league {league!r}")


def season_dates(league: str, season: int, today: date) -> list:
    """Every date in one season's window, through today."""
    start, end = season_window(league, season)
    last = min(end, today)
    out = []
    d = start
    while d <= last:
        out.append(d)
        d += timedelta(days=1)
    return out


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


def _prior_state(conn, event_ids) -> dict:
    """State of these games before this scoreboard is written. A nightly run
    has team boxes loaded from the published file and no stored summary. A
    resumed backfill has the summary and no need to fetch it again."""
    if not event_ids:
        return {}
    q = ("SELECT g.espn_event_id, g.completed, g.home_score, g.away_score, "
         "EXISTS(SELECT 1 FROM game_summaries s WHERE s.espn_event_id = g.espn_event_id), "
         "EXISTS(SELECT 1 FROM team_game_box b WHERE b.espn_event_id = g.espn_event_id "
         "AND b.stats_json IS NOT NULL) "
         f"FROM games g WHERE g.espn_event_id IN ({', '.join('?' * len(event_ids))})")
    out = {}
    for eid, comp, hs, aws, has_summary, has_box in conn.execute(q, tuple(event_ids)):
        out[int(eid)] = {
            "completed": bool(comp),
            "home_score": hs,
            "away_score": aws,
            "has_summary": bool(has_summary),
            "has_box": bool(has_box),
        }
    return out


def summaries_to_fetch(games, prior) -> list:
    """Completed games that need a summary. A game already final, with the
    same score, is skipped when its summary is stored or its team box was
    loaded from the published file. A new game, a game that just finished,
    or a game whose score changed is fetched."""
    out = []
    for g in games:
        if g.get("season_type") not in KEEP_SEASON_TYPES or not g.get("completed"):
            continue
        eid = g["espn_event_id"]
        prev = prior.get(eid)
        if prev is None:
            out.append(eid)
            continue
        same = (prev.get("home_score") == g.get("home_score")
                and prev.get("away_score") == g.get("away_score"))
        if prev.get("completed") and same and (prev.get("has_summary") or prev.get("has_box")):
            continue
        out.append(eid)
    return out


class SummaryFetchError(RuntimeError):
    """Some summaries failed. `stored` is the ones that succeeded, kept so a
    rerun does not download them again. The message has no URL and no token."""

    def __init__(self, stored, failed):
        self.stored = stored
        self.failed = failed
        eid, exc = failed[0]
        super().__init__(
            f"{len(failed)} summary fetches failed "
            f"(first event {eid}: {type(exc).__name__})")


def map_summaries(league, event_ids, fetch, workers):
    """Fetch summaries with at most `workers` requests in flight. `fetch` is
    called as fetch(league, event_id) and must not touch the database."""
    ids = list(event_ids)
    if not ids:
        return []
    workers = max(1, min(int(workers or 1), len(ids)))

    def one(eid):
        return eid, fetch(league, eid)

    if workers == 1:
        stored = []
        for eid in ids:
            try:
                stored.append(one(eid))
            except Exception as exc:
                raise SummaryFetchError(stored, [(eid, exc)]) from exc
        return stored
    stored, failed = [], []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(one, eid): eid for eid in ids}
        for fut in as_completed(futs):
            eid = futs[fut]
            try:
                stored.append(fut.result())
            except Exception as exc:
                failed.append((eid, exc))
    if failed:
        raise SummaryFetchError(stored, failed)
    return stored


def public_error(exc) -> str:
    """Error text safe to print: no URL, host, address, or token."""
    msg = str(exc)
    low = msg.lower()
    if ("://" in msg or "http" in low or "bearer " in low or "github_pat_" in low
            or "ghp_" in low or "gho_" in low or "token=" in low):
        return type(exc).__name__
    if msg.count(".") >= 3 and any(ch.isdigit() for ch in msg):
        return type(exc).__name__
    return f"{type(exc).__name__}: {msg[:180]}"


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
    """Fill each team's main home venue for the season.

    The scoreboard name stored on the way in is kept. A team with no
    scoreboard name yet (a nightly load of a file that was already named)
    gets the name on the teams row, which is the name that file used."""
    rows = conn.execute("""
        SELECT v.home_team_id, v.season, t.name, t.abbreviation, v.venue, v.n
        FROM (SELECT home_team_id, season, venue, COUNT(*) n,
                     ROW_NUMBER() OVER (PARTITION BY home_team_id, season
                                        ORDER BY COUNT(*) DESC) rk
              FROM games WHERE league = ? AND neutral_site = 0
                AND season_type = 2 AND venue IS NOT NULL
              GROUP BY home_team_id, season, venue) v
        JOIN teams t ON t.team_id = v.home_team_id
        WHERE v.rk = 1""", (league,)).fetchall()
    conn.executemany("""
        INSERT INTO team_seasons (team_id, league, season, name, abbreviation,
                                  home_venue, home_games)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (team_id, season) DO UPDATE SET
            home_venue = excluded.home_venue,
            home_games = excluded.home_games
    """, [(team_id, league, season, name, abbr, venue, n)
          for team_id, season, name, abbr, venue, n in rows])
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


def _store_fetched(conn, league, fetched) -> None:
    for eid, payload in fetched:
        store_summary(conn, league, eid, payload)
    if fetched:
        conn.commit()


def past_deadline(deadline, now=None) -> bool:
    """True when a one-season run should stop before the next date.

    Stopping between dates leaves that date unfinished, so the next run
    continues from the cache. `deadline` is a monotonic clock value."""
    if deadline is None:
        return False
    return (time.monotonic() if now is None else now) >= deadline


def run(conn, leagues, n_seasons, rate=1.0, today=None, only_dates=None,
        est_games_per_date=None, summary_workers=1, only_season=None,
        deadline=None) -> dict:
    today = today or date.today()
    pace = Pacer(rate)
    todo = plan(conn, leagues, n_seasons, today, only_dates)
    total_dates = sum(len(v) for v in todo.values())
    # rough per-date game estimate for the ETA until real counts accumulate
    est = est_games_per_date or {
        "mlb": 9, "nba": 4.5, "nfl": 1.4, "nhl": 5.5, "cfb": 12, "cbb": 40,
    }
    log(f"plan: {total_dates} dates to fetch "
        + ", ".join(f"{lg}={len(v)}" for lg, v in todo.items()))
    stats = {"dates": 0, "games": 0, "summaries": 0, "errors": 0, "removed": 0,
             "stopped_early": False}
    dates_left = total_dates

    for lg in leagues:
        started = _now()
        if not db.team_id_map(conn, lg):
            db.upsert_teams(conn, lg, espn.parse_teams(pace(espn.fetch_teams, lg)),
                            in_directory=True)
        db.link_franchises(conn, lg)
        lg_games = 0
        plog = {}
        for i, d in enumerate(todo[lg]):
            if past_deadline(deadline):
                stats["stopped_early"] = True
                log(f"{lg} stopping before {d.isoformat()} so this run can finish")
                break
            ds = d.isoformat()
            try:
                params = espn.scoreboard_params(lg, dates=f"{d:%Y%m%d}")
                data = pace(espn.fetch_scoreboard, lg, dates=f"{d:%Y%m%d}")
                n_events = len(data.get("events") or []) if isinstance(data, dict) else 0
                if n_events >= int(params["limit"]):
                    log(f"{lg} {ds} scoreboard returned {n_events} events, "
                        f"at the limit of {params['limit']}")
                parsed = espn.parse_scoreboard(data, lg)
                parsed["games"] = [g for g in parsed["games"]
                                   if g["season_type"] in KEEP_SEASON_TYPES]
                if only_season is not None:
                    parsed["games"] = [g for g in parsed["games"]
                                       if g.get("season") == only_season]
                ids = [g["espn_event_id"] for g in parsed["games"]]
                # Read the published final before ingest overwrites the score.
                prior = _prior_state(conn, ids)
                n = db.ingest_scoreboard(conn, lg, parsed)
                if d >= today - timedelta(days=1):
                    gone = reconcile_date(conn, lg, d, data, today)
                    stats["removed"] += len(gone)
                    # probables / odds / weather from the same payload (logging only)
                    prelog._add(plog, prelog.log_scoreboard(conn, lg, data, log=log))
                need = summaries_to_fetch(parsed["games"], prior)
                try:
                    if summary_workers <= 1:
                        fetched = map_summaries(
                            lg, need, lambda l, e: pace(espn.fetch_summary, l, e), 1)
                    else:
                        fetched = map_summaries(lg, need, espn.fetch_summary, summary_workers)
                        pace.n += len(fetched)
                except SummaryFetchError as err:
                    _store_fetched(conn, lg, err.stored)
                    stats["summaries"] += len(err.stored)
                    if summary_workers > 1:
                        pace.n += len(err.stored)
                    raise
                _store_fetched(conn, lg, fetched)
                stats["summaries"] += len(fetched)
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
                log(f"{lg} {ds} ERROR {public_error(e)}")
            stats["dates"] += 1
            dates_left -= 1
            if (i + 1) % 25 == 0 or i + 1 == len(todo[lg]):
                rem = sum(len(todo[x]) * (1 + est.get(x, 5)) for x in
                          leagues[leagues.index(lg) + 1:])
                rem += (len(todo[lg]) - i - 1) * (1 + est.get(lg, 5))
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
                    help="minimum seconds between scoreboard requests")
    ap.add_argument("--summary-workers", type=int, default=1,
                    help="summary requests in flight (1 keeps them on --rate)")
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
    stats = run(conn, leagues, args.seasons, rate=args.rate, only_dates=only,
                summary_workers=args.summary_workers)
    log(f"finished: {stats}")


if __name__ == "__main__":
    main()
