"""Pre-game data logging (logging only: nothing here feeds the model,
forecasts or site pages).

    python -m bayesratings.prelog rosters [--leagues nba,nhl]   # rosters + injuries
    python -m bayesratings.prelog depth                          # NFL depth charts
    python -m bayesratings.prelog transactions
    python -m bayesratings.prelog morning                        # all three
    python -m bayesratings.prelog pregame                        # games starting in 60-80 min
    python -m bayesratings.prelog status
    python -m bayesratings.prelog sources | goalies | nhl-lines | nbainj | nflverse
    python -m bayesratings.prelog check                          # end-to-end pipeline check
    python -m bayesratings.prelog nbainj-backfill --from 2025-10-21 --to 2026-06-30
    python -m bayesratings.prelog nbainj-rematch     # re-try open unmatched report games/players

The gap sources (MLB Stats API, Daily Faceoff, the NBA injury report PDF,
nflverse, NWS / Open-Meteo, plus NFL inactives and NBA starters from ESPN)
live in bayesratings/sources; see sources/runner.py for their schedule.

What is logged, and how:

  scoreboard (no extra requests: the hourly refresh and the 6 AM job pass the
  scoreboards they already fetch to log_scoreboard)
      game_probables   MLB probable pitchers, NHL probable goalies (with
                       ESPN's Expected/Confirmed status), any other probables
      game_odds        ESPN's betting block per provider (moneyline, spread,
                       total, open and current)
      game_weather     ESPN's forecast for outdoor venues
  per-team rosters (teams/{id}/roster)
      roster_states    one row per player state: team, status, position,
                       jersey, injury status + date
      injury_details   type / detail / side / return date for each injury the
                       roster shows, fetched once per new injury (core API)
  nfl_depth_states     NFL depth charts (teams/{id}/depthcharts)
  transactions         league transaction wire, deduplicated
  pregame pass (60-80 min before each start: that game's summary, plus one
  scoreboard per league and date so NHL goalie confirmations are fresh)
      pregame_snapshots  one row per game per fetch: weather, counts, the
                         summary (gzip, news/videos dropped)
      pregame_lineups    MLB batting order + SP, starters, probables, ...
      pregame_injuries   the injury list on the game page at that moment

Change-only tables (game_probables, game_odds, game_weather, roster_states,
injury_details, nfl_depth_states) keep one row per distinct state:
valid_from = first fetch that saw it, last_seen = latest fetch that still saw
it, valid_to = the fetch that saw it change or disappear (NULL = current). The
state at time T is valid_from <= T AND (valid_to IS NULL OR valid_to > T).
State changes between two fetches are only known to within that gap.

Every pass is recorded in prelog_runs (requests, rows changed, errors) and
appends one line to the status file. Writes run in short BEGIN IMMEDIATE
transactions so they queue behind the poller instead of failing.
"""

import argparse
import gzip
import hashlib
import json
import re
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path

from . import db, espn

LEAGUES = ("mlb", "nba", "nfl", "nhl")
CORE_API = "https://sports.core.api.espn.com/v2/sports"
PREGAME_MIN, PREGAME_MAX = 60, 80          # minutes before the start (cron every 10 min)
PREGAME_ACCEPT = (45, 105)                 # a snapshot in this band counts as done
DETAIL_CAP = 150                           # injury detail fetches per pass
SUMMARY_DROP = ("news", "videos", "article", "standings", "leaders",
                "lastFiveGames", "seasonseries", "ticketsInfo", "plays",
                "againstTheSpread", "meta", "broadcasts", "winprobability")
_SLIM = {"links", "logos", "logo", "headshot", "uid", "guid", "$ref", "alternateIds",
         "contracts", "contract", "college", "collegeAthlete", "birthPlace", "flag",
         "images", "footer", "header", "tracking", "link", "draft"}
STATUS_FILE = "/opt/status/bayesratings-prelog.log"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def log(msg: str) -> None:
    print(f"[{datetime.now(espn.CENTRAL):%Y-%m-%d %H:%M:%S} CT] {msg}", flush=True)


def slim(x):
    """Drop links, images and other bulky noise; keep everything else."""
    if isinstance(x, dict):
        return {k: slim(v) for k, v in x.items() if k not in _SLIM}
    if isinstance(x, list):
        return [slim(v) for v in x]
    return x


def _js(x) -> str:
    return json.dumps(slim(x), separators=(",", ":"), sort_keys=True)


def _int(v):
    if v in (None, ""):
        return None
    s = str(v).strip().upper()
    if s in ("EVEN", "EV", "PK"):
        return 100
    try:
        return int(float(s.lstrip("+")))
    except ValueError:
        return None


def _float(v):
    if v in (None, ""):
        return None
    try:
        return float(str(v).strip().lstrip("+ouOU"))
    except ValueError:
        return None


# ------------------------------------------------------------- write helpers

class Tx:
    """Short write transaction that waits for the write lock (busy_timeout)
    instead of failing when the poller committed in between (see PR #45)."""

    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        self.conn.commit()
        self.conn.execute("BEGIN IMMEDIATE")
        return self.conn

    def __exit__(self, exc_type, *_):
        if exc_type:
            self.conn.rollback()
        else:
            self.conn.commit()
        return False


def track(conn, table, key_cols, fields, rows, scope, seen_at) -> dict:
    """Change-only upsert. `rows` is the complete current set for `scope`
    (a {col: value} filter, e.g. one team's roster); `fields` are the typed
    columns whose change makes a new state. An open state with the same
    fields gets last_seen = seen_at; a changed one is closed (valid_to) and a
    new row inserted; an open state missing from `rows` is closed. Pass
    scope=None to never close missing keys. Call inside a transaction."""
    st = {"new": 0, "changed": 0, "closed": 0, "same": 0}
    where = " AND ".join(f"{c} = ?" for c in scope) if scope else "1"
    args = tuple(scope.values()) if scope else ()
    open_ = {}
    if scope or rows:
        q = f"SELECT id, {', '.join(key_cols)}, state_hash FROM {table} WHERE valid_to IS NULL"
        if scope:
            q += f" AND {where}"
            res = conn.execute(q, args)
        else:   # no scope: only the keys we were given
            res = []
            for r in rows:
                res += conn.execute(q + " AND " + " AND ".join(f"{c} = ?" for c in key_cols),
                                    tuple(r[c] for c in key_cols)).fetchall()
        for rec in res:
            open_[tuple(rec[1:1 + len(key_cols)])] = (rec[0], rec[-1])
    meta = ("id", "raw_json", "state_hash", "valid_from", "valid_to", "last_seen")
    # every table column the rows carry (keys, fields and context like league)
    data_cols = [c for c in _columns(conn, table) if c not in meta]
    cols = data_cols + ["raw_json", "state_hash", "valid_from", "last_seen"]
    ins = (f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))})")
    seen = set()
    for r in rows:
        key = tuple(r[c] for c in key_cols)
        if key in seen:
            continue
        seen.add(key)
        h = hashlib.sha1(json.dumps([r.get(f) for f in fields], default=str).encode()).hexdigest()[:16]
        cur = open_.get(key)
        if cur and cur[1] == h:
            conn.execute(f"UPDATE {table} SET last_seen = ? WHERE id = ?", (seen_at, cur[0]))
            st["same"] += 1
            continue
        if cur:
            conn.execute(f"UPDATE {table} SET valid_to = ? WHERE id = ?", (seen_at, cur[0]))
            st["changed"] += 1
        else:
            st["new"] += 1
        conn.execute(ins, [r.get(c) for c in data_cols] + [r.get("raw_json"), h, seen_at, seen_at])
    if scope:
        for key, (rid, _) in open_.items():
            if key not in seen:
                conn.execute(f"UPDATE {table} SET valid_to = ? WHERE id = ?", (seen_at, rid))
                st["closed"] += 1
    return st


def _columns(conn, table):
    return [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]


def _add(tot, st):
    for k, v in st.items():
        tot[k] = tot.get(k, 0) + v
    return tot


def changed(counts) -> int:
    """Rows that changed (new + changed + closed states, new rows)."""
    return sum(v for k, v in counts.items()
               if k.split("_")[-1] in ("new", "changed", "closed") or k == "rows")


def record_scoreboard_run(conn, league, started, counts, log=log):
    """One prelog_runs row for the scoreboard logging of a refresh/daily run.
    Never raises."""
    try:
        record_run(conn, "scoreboard", league, started, None, counts, counts.get("error", 0))
    except Exception as e:
        log(f"prelog scoreboard {league} run log ERROR {type(e).__name__}: {e}")


def record_run(conn, kind, league, started, requests, counts, errors, note=None):
    with Tx(conn):
        conn.execute(
            "INSERT INTO prelog_runs (kind, league, started_at, finished_at, requests, "
            "changed, errors, counts_json, note) VALUES (?,?,?,?,?,?,?,?,?)",
            (kind, league, started, _now(), requests,
             changed(counts), errors, json.dumps(counts, sort_keys=True), note))


# ------------------------------------------------------------- scoreboard

PRE_GAME = ("STATUS_SCHEDULED",)


def parse_scoreboard(data, league):
    """{espn_event_id: {"home": espn id, "away": espn id, "probables": [...],
    "odds": [...], "weather": {...} or None}} for games not yet started."""
    out = {}
    for ev in (data or {}).get("events") or []:
        try:
            comp = ev["competitions"][0]
            if (comp.get("status") or ev.get("status") or {}).get("type", {}).get("name") not in PRE_GAME:
                continue
            eid = int(ev["id"])
            sides = {c["homeAway"]: c for c in comp["competitors"]}
            probs = []
            for c in comp["competitors"]:
                for p in c.get("probables") or []:
                    a = p.get("athlete") or {}
                    probs.append(dict(
                        espn_team=int(c["team"]["id"]), kind=p.get("abbreviation") or p.get("name"),
                        athlete_id=_int(a.get("id") or p.get("playerId")),
                        name=a.get("displayName") or a.get("fullName"),
                        status=(p.get("status") or {}).get("name"), raw=p))
            out[eid] = dict(home=int(sides["home"]["team"]["id"]),
                            away=int(sides["away"]["team"]["id"]), probables=probs,
                            odds=[parse_odds(o) for o in comp.get("odds") or []],
                            weather=parse_weather(ev.get("weather") or comp.get("weather")))
        except (KeyError, TypeError, ValueError):
            continue
    return out


def _side(block, side, when, key):
    return (((block or {}).get(side) or {}).get(when) or {}).get(key)


def parse_odds(o):
    ml, ps, tot = o.get("moneyline"), o.get("pointSpread"), o.get("total")
    prov = o.get("provider") or {}
    return dict(
        provider_id=_int(prov.get("id")) or 0, provider=prov.get("name"),
        details=o.get("details"), over_under=_float(o.get("overUnder")),
        home_ml=_int(_side(ml, "home", "close", "odds")), away_ml=_int(_side(ml, "away", "close", "odds")),
        home_ml_open=_int(_side(ml, "home", "open", "odds")), away_ml_open=_int(_side(ml, "away", "open", "odds")),
        home_spread=_float(_side(ps, "home", "close", "line")),
        home_spread_odds=_int(_side(ps, "home", "close", "odds")),
        away_spread_odds=_int(_side(ps, "away", "close", "odds")),
        home_spread_open=_float(_side(ps, "home", "open", "line")),
        total=_float(_side(tot, "over", "close", "line")),
        over_odds=_int(_side(tot, "over", "close", "odds")),
        under_odds=_int(_side(tot, "under", "close", "odds")),
        total_open=_float(_side(tot, "over", "open", "line")),
        home_favorite=int(bool((o.get("homeTeamOdds") or {}).get("favorite"))),
        raw_json=_js(o))


def parse_weather(w):
    if not w:
        return None
    return dict(condition=w.get("displayValue"), condition_id=_int(w.get("conditionId")),
                temperature=_int(w.get("temperature")), high_temp=_int(w.get("highTemperature")),
                low_temp=_int(w.get("lowTemperature")), gust=_int(w.get("gust")),
                precipitation=_int(w.get("precipitation")), raw_json=_js(w))


PROB_FIELDS = ("athlete_id", "name", "status")
ODDS_FIELDS = ("provider", "details", "over_under", "home_ml", "away_ml", "home_ml_open",
               "away_ml_open", "home_spread", "home_spread_odds", "away_spread_odds",
               "home_spread_open", "total", "over_odds", "under_odds", "total_open",
               "home_favorite")
WEATHER_FIELDS = ("condition", "condition_id", "temperature", "high_temp", "low_temp",
                  "gust", "precipitation")


def store_scoreboard(conn, league, parsed, fetched_at, source="scoreboard") -> dict:
    """Change-only rows for probables, odds and weather of pre-game games.
    Only games we already know (games table) are logged."""
    tid = db.team_id_map(conn, league)
    known = {r[0] for r in conn.execute(
        "SELECT espn_event_id FROM games WHERE league = ? AND espn_event_id IN (%s)"
        % ",".join("?" * len(parsed)), (league, *parsed))} if parsed else set()
    tot = {}
    with Tx(conn):
        for eid, g in parsed.items():
            if eid not in known:
                continue
            probs = [dict(espn_event_id=eid, league=league, team_id=tid.get(p["espn_team"]),
                          kind=p["kind"] or "?", athlete_id=p["athlete_id"], name=p["name"],
                          status=p["status"], raw_json=_js(p["raw"]))
                     for p in g["probables"] if tid.get(p["espn_team"])]
            # a probable ESPN stops listing is closed (scope = the game)
            _add(tot, {f"probables_{k}": v for k, v in track(
                conn, "game_probables", ("espn_event_id", "team_id", "kind"), PROB_FIELDS,
                probs, {"espn_event_id": eid}, fetched_at).items()})
            odds = [dict(o, espn_event_id=eid, league=league) for o in g["odds"]]
            _add(tot, {f"odds_{k}": v for k, v in track(
                conn, "game_odds", ("espn_event_id", "provider_id"), ODDS_FIELDS, odds,
                {"espn_event_id": eid}, fetched_at).items()})
            if g["weather"]:
                w = dict(g["weather"], espn_event_id=eid, league=league, source=source)
                _add(tot, {f"weather_{k}": v for k, v in track(
                    conn, "game_weather", ("espn_event_id", "source"), WEATHER_FIELDS, [w],
                    None, fetched_at).items()})
    return tot


def log_scoreboard(conn, league, data, fetched_at=None, log=log) -> dict:
    """Hook for the scoreboards the refresh and daily jobs already fetch.
    Never raises: a logging failure must not break those jobs."""
    try:
        return store_scoreboard(conn, league, parse_scoreboard(data, league),
                                fetched_at or _now())
    except Exception as e:
        log(f"prelog scoreboard {league} ERROR {type(e).__name__}: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return {"error": 1}


# ------------------------------------------------------------- rosters

def fetch_roster(league, espn_team):
    cfg = espn.LEAGUES[league]
    return espn._get(f"{espn.SITE_API}/{cfg['sport']}/{cfg['league']}/teams/{espn_team}/roster")


def fetch_team_injuries(league, espn_team):
    cfg = espn.LEAGUES[league]
    return espn._get(f"{CORE_API}/{cfg['sport']}/leagues/{cfg['league']}/teams/{espn_team}/injuries",
                     {"limit": 200})


def fetch_ref(url):
    return espn._get(url.replace("http://", "https://", 1))


def roster_athletes(data):
    """Flatten ESPN's roster (grouped by position for MLB/NFL/NHL, flat for
    NBA) into (group, athlete) pairs."""
    out = []
    for a in (data or {}).get("athletes") or []:
        if isinstance(a, dict) and "items" in a:
            out += [(a.get("position"), x) for x in a.get("items") or []]
        elif isinstance(a, dict):
            out.append((None, a))
    return out


ROSTER_FIELDS = ("name", "position", "jersey", "roster_group", "status",
                 "injury_status", "injury_date")


def parse_roster(data, league, team_id):
    rows = []
    for group, a in roster_athletes(data):
        if not a.get("id"):
            continue
        inj = (a.get("injuries") or [{}])[0] or {}
        pos = a.get("position") or {}
        rows.append(dict(
            league=league, team_id=team_id, athlete_id=int(a["id"]),
            name=a.get("displayName") or a.get("fullName"),
            position=pos.get("abbreviation") if isinstance(pos, dict) else None,
            jersey=a.get("jersey"), roster_group=group,
            status=(a.get("status") or {}).get("name"),
            injury_status=inj.get("status"), injury_date=inj.get("date"),
            raw_json=_js({k: v for k, v in a.items()
                          if k not in ("statistics", "age", "experience")})))
    return rows


INJ_FIELDS = ("team_id", "athlete_id", "status", "injury_date", "type", "detail", "side",
              "return_date", "fantasy_status", "short_comment")
# ESPN's list is newest first; some entry ids are negative
_INJ_REF = re.compile(r"/athletes/(\d+)/injuries/(-?\d+)")


def parse_injury_detail(d, league, team_id, athlete_id):
    det = d.get("details") or {}
    return dict(league=league, injury_id=_int(d.get("id")), team_id=team_id,
                athlete_id=athlete_id, status=d.get("status"), injury_date=d.get("date"),
                type=det.get("type") or (d.get("type") or {}).get("description"),
                detail=det.get("detail"), side=det.get("side"),
                return_date=det.get("returnDate"),
                fantasy_status=(det.get("fantasyStatus") or {}).get("description"),
                short_comment=d.get("shortComment"), raw_json=_js(d))


TEAM_WINDOW = (-7, 14)          # days around today a team must have a game in


def _teams(conn, league, today=None):
    """(espn_id, team_id) of teams with a regular/postseason game within
    TEAM_WINDOW days: everyone in season, only the teams still playing in
    the playoffs, nobody in the off-season."""
    today = today or datetime.now(espn.CENTRAL).date()
    lo, hi = (today + timedelta(days=d) for d in TEAM_WINDOW)
    ex = db.exhibition_team_ids(conn, league)
    return [(r[0], r[1]) for r in conn.execute(
        "SELECT DISTINCT t.espn_id, t.team_id FROM teams t JOIN games g "
        "ON t.team_id IN (g.home_team_id, g.away_team_id) WHERE t.league = ? "
        "AND g.season_type IN (2, 3) AND g.game_date BETWEEN ? AND ? ORDER BY t.espn_id",
        (league, lo.isoformat(), hi.isoformat())) if r[1] not in ex]


def rosters_pass(conn, league, pace, log=log, detail_cap=DETAIL_CAP) -> dict:
    """One roster + injury snapshot of every current team in the league."""
    tot, errors, details = {}, 0, 0
    teams = _teams(conn, league)
    tot["teams"] = len(teams)
    for espn_team, team_id in teams:
        try:
            data = pace(fetch_roster, league, espn_team)
            at = _now()
            rows = parse_roster(data, league, team_id)
            if not rows:
                raise ValueError("empty roster")
            with Tx(conn):
                _add(tot, track(conn, "roster_states", ("league", "team_id", "athlete_id"),
                                ROSTER_FIELDS, rows, {"league": league, "team_id": team_id}, at))
            # injury details: one core-API lookup per roster injury state
            # (status + date) ever; a miss is recorded too, so it isn't retried
            need = [r for r in rows if r["injury_status"] and not conn.execute(
                "SELECT 1 FROM injury_lookups WHERE league = ? AND athlete_id = ? "
                "AND roster_status = ? AND roster_injury_date IS ?",
                (league, r["athlete_id"], r["injury_status"], r["injury_date"])).fetchone()]
            if need and details < detail_cap:
                refs = pace(fetch_team_injuries, league, espn_team)
                latest = {}
                for it in refs.get("items") or []:
                    m = _INJ_REF.search(it.get("$ref", ""))
                    if m:
                        aid, iid = int(m.group(1)), int(m.group(2))
                        if aid not in latest:          # list order = newest first
                            latest[aid] = (iid, it["$ref"])
                got, looked = [], []
                for r in need:
                    aid = r["athlete_id"]
                    if details >= detail_cap:
                        break                       # the rest wait for the next pass
                    if aid not in latest:
                        tot["detail_missing"] = tot.get("detail_missing", 0) + 1
                        looked.append((r, None))
                        continue
                    d = pace(fetch_ref, latest[aid][1])
                    details += 1
                    row = parse_injury_detail(d, league, team_id, aid)
                    if row["injury_id"]:
                        got.append(row)
                    looked.append((r, row["injury_id"]))
                at = _now()
                with Tx(conn):
                    _add(tot, {f"injury_{k}": v for k, v in track(
                        conn, "injury_details", ("league", "injury_id"), INJ_FIELDS,
                        got, None, at).items()})
                    conn.executemany(
                        "INSERT OR REPLACE INTO injury_lookups (league, athlete_id, roster_status, "
                        "roster_injury_date, injury_id, checked_at) VALUES (?,?,?,?,?,?)",
                        [(league, r["athlete_id"], r["injury_status"], r["injury_date"], iid, at)
                         for r, iid in looked])
        except Exception as e:
            errors += 1
            log(f"prelog rosters {league} team {espn_team} ERROR {type(e).__name__}: {e}")
    tot["injury_detail_fetches"] = details
    return dict(tot, errors=errors)


# ------------------------------------------------------------- NFL depth

DEPTH_FIELDS = ("athlete_id", "name", "position")


def fetch_depth(espn_team):
    return espn._get(f"{espn.SITE_API}/football/nfl/teams/{espn_team}/depthcharts")


def parse_depth(data, team_id):
    rows = []
    for f in (data or {}).get("depthchart") or []:
        formation = f.get("name") or f.get("id")
        for slot, p in (f.get("positions") or {}).items():
            for rank, a in enumerate(p.get("athletes") or [], 1):
                if not a.get("id"):
                    continue
                rows.append(dict(league="nfl", team_id=team_id, formation=formation, slot=slot,
                                 depth=rank, athlete_id=int(a["id"]), name=a.get("displayName"),
                                 position=(p.get("position") or {}).get("abbreviation"),
                                 raw_json=_js(a)))
    return rows


def depth_pass(conn, pace, log=log) -> dict:
    tot, errors = {}, 0
    for espn_team, team_id in _teams(conn, "nfl"):
        try:
            rows = parse_depth(pace(fetch_depth, espn_team), team_id)
            if not rows:
                raise ValueError("empty depth chart")
            with Tx(conn):
                _add(tot, track(conn, "nfl_depth_states",
                                ("league", "team_id", "formation", "slot", "depth"),
                                DEPTH_FIELDS, rows, {"league": "nfl", "team_id": team_id}, _now()))
        except Exception as e:
            errors += 1
            log(f"prelog depth team {espn_team} ERROR {type(e).__name__}: {e}")
    return dict(tot, errors=errors)


# ------------------------------------------------------------- transactions

def fetch_transactions(league, page=1, limit=200):
    cfg = espn.LEAGUES[league]
    return espn._get(f"{espn.SITE_API}/{cfg['sport']}/{cfg['league']}/transactions",
                     {"limit": limit, "page": page})


def parse_transactions(data, league, tid):
    out = []
    for t in (data or {}).get("transactions") or []:
        team = t.get("team") or {}
        eteam = _int(team.get("id"))
        desc = (t.get("description") or "").strip()
        key = hashlib.sha1(f"{league}|{t.get('date')}|{eteam}|{desc}".encode()).hexdigest()[:20]
        out.append(dict(league=league, txn_key=key, txn_date=t.get("date"), espn_team_id=eteam,
                        team_id=tid.get(eteam), description=desc,
                        raw_json=_js({k: v for k, v in t.items() if k != "team"})))
    return out


def transactions_pass(conn, league, pace, max_pages=5, log=log) -> dict:
    tid = db.team_id_map(conn, league)
    new = seen = 0
    for page in range(1, max_pages + 1):
        rows = parse_transactions(pace(fetch_transactions, league, page), league, tid)
        at = _now()
        with Tx(conn):
            before = conn.total_changes
            conn.executemany(
                "INSERT OR IGNORE INTO transactions (league, txn_key, txn_date, espn_team_id, "
                "team_id, description, raw_json, first_seen) VALUES (?,?,?,?,?,?,?,?)",
                [(r["league"], r["txn_key"], r["txn_date"], r["espn_team_id"], r["team_id"],
                  r["description"], r["raw_json"], at) for r in rows])
            added = conn.total_changes - before
        new += added
        seen += len(rows)
        # stop once a page holds something we already had (we've caught up)
        if added < len(rows) or len(rows) < 200:
            break
    return {"rows": new, "seen": seen}


# ------------------------------------------------------------- pre-game pass

def due_games(conn, now=None, lo=PREGAME_MIN, hi=PREGAME_MAX):
    """Unstarted, playable games starting in [lo, hi] minutes with no
    snapshot yet taken PREGAME_ACCEPT minutes before their *current* start
    (a moved start re-qualifies)."""
    now = now or datetime.now(timezone.utc)
    a = (now + timedelta(minutes=lo)).strftime("%Y-%m-%dT%H:%M")
    b = (now + timedelta(minutes=hi)).strftime("%Y-%m-%dT%H:%MZ")
    rows = conn.execute(
        "SELECT g.espn_event_id, g.league, g.start_time_utc, g.game_date FROM games g "
        f"WHERE g.completed = 0 AND {db.playable_sql('g')} AND g.start_time_utc >= ? "
        "AND g.start_time_utc <= ? ORDER BY g.start_time_utc", (a, b)).fetchall()
    out = []
    for eid, lg, start, gdate in rows:
        st = _utc(start)
        lo_t = (st - timedelta(minutes=PREGAME_ACCEPT[1])).isoformat(timespec="seconds")
        hi_t = (st - timedelta(minutes=PREGAME_ACCEPT[0])).isoformat(timespec="seconds")
        done = conn.execute("SELECT 1 FROM pregame_snapshots WHERE espn_event_id = ? "
                            "AND fetched_at BETWEEN ? AND ?", (eid, lo_t, hi_t)).fetchone()
        if not done:
            out.append((eid, lg, start, gdate))
    return out


def _utc(s):
    s = s.replace("Z", "+00:00")
    d = datetime.fromisoformat(s)
    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)


def parse_pregame(summary, league, tid):
    """Lineups/starters/probables and injuries as the game page lists them."""
    lineups, injuries = [], []

    def team_of(t):
        return tid.get(_int(((t or {}).get("team") or {}).get("id")))

    # MLB lineups (and any league ESPN fills `rosters` for before the game)
    for t in summary.get("rosters") or []:
        team = team_of(t)
        for e in t.get("roster") or []:
            a = e.get("athlete") or {}
            if not a.get("id") or team is None:
                continue
            pos = (e.get("position") or {}).get("abbreviation")
            # MLB: the batting order (starter + batOrder); the starting
            # pitcher is the header probable below
            role = "lineup" if e.get("starter") else ("inactive" if e.get("active") is False else "bench")
            lineups.append(dict(team_id=team, athlete_id=int(a["id"]), name=a.get("displayName"),
                                role=role, order_no=_int(e.get("batOrder")), position=pos,
                                status=None, raw_json=_js({k: v for k, v in e.items() if k != "stats"})))
    # box score listings (NBA/NHL starters once ESPN posts them; inactive / DNP flags)
    for t in (summary.get("boxscore") or {}).get("players") or []:
        team = team_of(t)
        for g in t.get("statistics") or []:
            for e in g.get("athletes") or []:
                a = e.get("athlete") or {}
                if not a.get("id") or team is None:
                    continue
                if e.get("starter"):
                    role = "starter"
                elif e.get("didNotPlay") or e.get("active") is False:
                    role = "inactive"
                else:
                    continue
                lineups.append(dict(team_id=team, athlete_id=int(a["id"]), name=a.get("displayName"),
                                    role=role, order_no=None, position=(a.get("position") or {}).get("abbreviation"),
                                    status=e.get("reason") or None, group_name=g.get("name"),
                                    raw_json=_js({k: v for k, v in e.items() if k != "stats"})))
    # probables on the game page header (MLB starting pitchers)
    for c in ((summary.get("header") or {}).get("competitions") or [{}])[0].get("competitors") or []:
        team = tid.get(_int((c.get("team") or {}).get("id")))
        for p in c.get("probables") or []:
            a = p.get("athlete") or {}
            if a.get("id") and team is not None:
                lineups.append(dict(team_id=team, athlete_id=int(a["id"]), name=a.get("displayName"),
                                    role="probable", order_no=None, position=p.get("abbreviation"),
                                    status=(p.get("status") or {}).get("name"), raw_json=_js(p)))
    for t in summary.get("injuries") or []:
        team = team_of(t)
        for i in t.get("injuries") or []:
            a = i.get("athlete") or {}
            if not a.get("id") or team is None:
                continue
            det = i.get("details") or {}
            injuries.append(dict(team_id=team, athlete_id=int(a["id"]), name=a.get("displayName"),
                                 status=i.get("status"), injury_date=i.get("date"),
                                 type=det.get("type") or (i.get("type") or {}).get("description"),
                                 detail=det.get("detail"), side=det.get("side"),
                                 return_date=det.get("returnDate"),
                                 raw_json=_js({k: v for k, v in i.items() if k != "athlete"})))
    # one row per (team, role, athlete)
    uniq = {}
    for r in lineups:
        uniq.setdefault((r["team_id"], r["role"], r["athlete_id"]), r)
    w = (summary.get("gameInfo") or {}).get("weather")
    return list(uniq.values()), injuries, parse_weather(w)


def store_pregame(conn, eid, league, start, summary, fetched_at):
    tid = db.team_id_map(conn, league)
    lineups, injuries, weather = parse_pregame(summary, league, tid)
    status = (((summary.get("header") or {}).get("competitions") or [{}])[0]
              .get("status") or {}).get("type", {}).get("name")
    mins = round((_utc(start) - _utc(fetched_at)).total_seconds() / 60)
    body = {k: v for k, v in summary.items() if k not in SUMMARY_DROP}
    raw = json.dumps(body, separators=(",", ":")).encode()
    with Tx(conn):
        conn.execute(
            "INSERT OR REPLACE INTO pregame_snapshots (espn_event_id, fetched_at, league, "
            "start_time_utc, minutes_before, status, n_lineup, n_injuries, weather_condition_id, "
            "temperature, gust, precipitation, raw_bytes, summary_gz) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (eid, fetched_at, league, start, mins, status, len(lineups), len(injuries),
             (weather or {}).get("condition_id"), (weather or {}).get("temperature"),
             (weather or {}).get("gust"), (weather or {}).get("precipitation"),
             len(raw), gzip.compress(raw)))
        conn.executemany(
            "INSERT OR REPLACE INTO pregame_lineups (espn_event_id, fetched_at, team_id, role, "
            "athlete_id, name, order_no, position, status, group_name, raw_json) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            [(eid, fetched_at, r["team_id"], r["role"], r["athlete_id"], r["name"], r["order_no"],
              r["position"], r["status"], r.get("group_name"), r["raw_json"]) for r in lineups])
        conn.executemany(
            "INSERT OR REPLACE INTO pregame_injuries (espn_event_id, fetched_at, team_id, "
            "athlete_id, name, status, injury_date, type, detail, side, return_date, raw_json) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            [(eid, fetched_at, r["team_id"], r["athlete_id"], r["name"], r["status"],
              r["injury_date"], r["type"], r["detail"], r["side"], r["return_date"],
              r["raw_json"]) for r in injuries])
        if weather:
            track(conn, "game_weather", ("espn_event_id", "source"), WEATHER_FIELDS,
                  [dict(weather, espn_event_id=eid, league=league, source="summary")],
                  None, fetched_at)
    return {"games": 1, "lineup": len(lineups), "injuries": len(injuries)}


def pregame_pass(conn, pace, leagues=LEAGUES, now=None, log=log) -> dict:
    """Snapshot every due game. `now` (tests) pins the clock and fetched_at."""
    stamp = (lambda: now.isoformat(timespec="seconds")) if now else _now
    due = [d for d in due_games(conn, now) if d[1] in leagues]
    res = {}
    # one scoreboard per (league, ESPN date) with due games: fresh NHL goalie
    # confirmations, MLB probables and odds right before the start
    for lg, edate in sorted({(lg, espn.eastern_date(start)) for _, lg, start, _ in due}):
        st = res.setdefault(lg, {"errors": 0})
        try:
            data = pace(espn.fetch_scoreboard, lg, dates=edate.replace("-", ""))
            _add(st, store_scoreboard(conn, lg, parse_scoreboard(data, lg), stamp()))
        except Exception as e:
            st["errors"] += 1
            log(f"prelog pregame scoreboard {lg} {edate} ERROR {type(e).__name__}: {e}")
    for eid, lg, start, _ in due:
        st = res.setdefault(lg, {"errors": 0})
        try:
            summary = pace(espn.fetch_summary, lg, eid)
            _add(st, store_pregame(conn, eid, lg, start, summary, stamp()))
        except Exception as e:
            st["errors"] += 1
            log(f"prelog pregame {lg} {eid} ERROR {type(e).__name__}: {e}")
    return res


# ------------------------------------------------------------- status

TABLES = ("game_probables", "game_odds", "game_weather", "roster_states", "injury_details",
          "nfl_depth_states", "transactions", "pregame_snapshots")
_TS = {"transactions": "first_seen", "pregame_snapshots": "fetched_at"}


def status(conn) -> dict:
    """Last run per kind/league and row counts per table per league."""
    runs = {}
    for r in conn.execute(
            "SELECT kind, league, MAX(finished_at), requests, changed, errors FROM prelog_runs "
            "GROUP BY kind, league ORDER BY kind, league"):
        runs[f"{r[0]}/{r[1]}"] = {"last": r[2], "requests": r[3], "changed": r[4], "errors": r[5]}
    tables = {}
    for t in TABLES:
        ts = _TS.get(t, "last_seen")
        cur = "SUM(valid_to IS NULL)" if t not in _TS else "NULL"
        tables[t] = {r[0]: {"rows": r[1], "current": r[2], "latest": r[3]} for r in conn.execute(
            f"SELECT league, COUNT(*), {cur}, MAX({ts}) FROM {t} GROUP BY league")}
    tables["pregame_lineups"] = {r[0]: {"rows": r[1]} for r in conn.execute(
        "SELECT s.league, COUNT(*) FROM pregame_lineups l JOIN pregame_snapshots s "
        "USING (espn_event_id, fetched_at) GROUP BY s.league")}
    tables["pregame_injuries"] = {r[0]: {"rows": r[1]} for r in conn.execute(
        "SELECT s.league, COUNT(*) FROM pregame_injuries l JOIN pregame_snapshots s "
        "USING (espn_event_id, fetched_at) GROUP BY s.league")}
    req24 = conn.execute("SELECT COALESCE(SUM(requests), 0) FROM prelog_runs WHERE "
                         "finished_at >= ?", ((datetime.now(timezone.utc) - timedelta(days=1))
                                              .isoformat(timespec="seconds"),)).fetchone()[0]
    return {"runs": runs, "tables": tables, "requests_24h": req24, "sources": source_status(conn)}


SOURCE_TABLES = {"mlb_roster_states": "last_seen", "mlb_transactions": "last_seen",
                 "mlb_lineup_states": "last_seen", "nhl_goalie_states": "last_seen",
                 "nhl_line_states": "last_seen", "nba_injury_reports": "fetched_at",
                 "nba_injury_states": "last_seen", "nfl_injury_report_states": "last_seen",
                 "nfl_weekly_roster_states": "last_seen", "nfl_inactives": "fetched_at",
                 "nba_starters": "fetched_at", "game_weather_fcst": "fetched_at", "venues": "updated_at"}


def source_status(conn) -> dict:
    """Per gap-source job: last run / last success / 24 h requests and
    errors; per table: rows and latest write; open unmatched ids."""
    from .sources.runner import JOBS
    since = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat(timespec="seconds")
    jobs = {}
    for name in JOBS:
        last, ok = conn.execute("SELECT MAX(finished_at), MAX(CASE WHEN errors = 0 THEN finished_at END) "
                                "FROM prelog_runs WHERE kind = ?", (name,)).fetchone()
        n, req, err, chg = conn.execute(
            "SELECT COUNT(*), COALESCE(SUM(requests), 0), COALESCE(SUM(errors), 0), "
            "COALESCE(SUM(changed), 0) FROM prelog_runs WHERE kind = ? AND finished_at >= ?",
            (name, since)).fetchone()
        jobs[name] = {"last_run": last, "last_success": ok, "runs_24h": n, "requests_24h": req,
                      "errors_24h": err, "changed_24h": chg}
    tables = {}
    for t, ts in SOURCE_TABLES.items():
        n, latest = conn.execute(f"SELECT COUNT(*), MAX({ts}) FROM {t}").fetchone()
        tables[t] = {"rows": n, "latest": latest}
    unm = {f"{r[0]}/{r[1]}": r[2] for r in conn.execute(
        "SELECT source, kind, COUNT(*) FROM ext_unmatched WHERE resolved_at IS NULL GROUP BY 1, 2")}
    return {"jobs": jobs, "tables": tables, "unmatched_open": unm}





def status_line(conn) -> str:
    s = status(conn)
    parts = [f"{t} " + ",".join(f"{lg}{v['rows']}" for lg, v in sorted(by.items()))
             for t, by in s["tables"].items() if by]
    src = s["sources"]
    srows = " ".join(f"{t.replace('_states', '')}{v['rows']}" for t, v in src["tables"].items() if v["rows"])
    unm = sum(src["unmatched_open"].values())
    return (f"rows: {' | '.join(parts) or 'none'} | src: {srows or 'none'} | unmatched {unm} | "
            f"{s['requests_24h']} req/24h")


def health(conn) -> str:
    """One short line for the daily status file: last 24 h of prelog runs."""
    since = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat(timespec="seconds")
    n_runs, errs, req = conn.execute(
        "SELECT COUNT(*), COALESCE(SUM(errors), 0), COALESCE(SUM(requests), 0) "
        "FROM prelog_runs WHERE finished_at >= ? AND league != 'all'", (since,)).fetchone()
    last = conn.execute("SELECT MAX(fetched_at) FROM pregame_snapshots").fetchone()[0]
    last = (f"{_utc(last).astimezone(espn.CENTRAL):%m-%d %H:%M} CT" if last else "none")
    line = f"prelog 24h {n_runs} runs {errs}err {req}req, last pregame {last}"
    chk = conn.execute("SELECT note FROM prelog_runs WHERE kind = 'check' ORDER BY finished_at DESC "
                       "LIMIT 1").fetchone()
    return line + (f", check {chk[0]}" if chk and chk[0] else "")


def write_status(path, line):
    if not path:
        return
    try:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        with p.open("a") as fh:
            fh.write(line + "\n")
    except OSError as e:
        log(f"could not write status file {path}: {e}")


# ------------------------------------------------------------- CLI

def run(conn, kind, leagues, pace, log=log) -> tuple:
    """Run one pass kind; returns (ok, summary parts)."""
    parts, failed = [], False
    jobs = []
    if kind in ("rosters", "morning"):
        jobs += [("rosters", lg) for lg in leagues]
    if kind in ("depth", "morning") and "nfl" in leagues:
        jobs.append(("depth", "nfl"))
    if kind in ("transactions", "morning"):
        jobs += [("transactions", lg) for lg in leagues]
    if kind == "pregame":
        jobs.append(("pregame", "all"))
    for job, lg in jobs:
        if job in ("rosters", "depth") and not _teams(conn, lg):
            continue                    # off-season: no roster requests
        started, n0 = _now(), pace.n
        try:
            if job == "rosters":
                st = rosters_pass(conn, lg, pace, log=log)
            elif job == "depth":
                st = depth_pass(conn, pace, log=log)
            elif job == "transactions":
                st = transactions_pass(conn, lg, pace, log=log)
            else:
                res = pregame_pass(conn, pace, leagues, log=log)
                for plg, pst in res.items():
                    record_run(conn, "pregame", plg, started, None, pst, pst.get("errors", 0))
                    failed |= bool(pst.get("errors"))
                parts.append("pregame " + (", ".join(
                    f"{plg} {v.get('games', 0)}g {v.get('lineup', 0)}lu"
                    + (f" {v['errors']}err" if v.get("errors") else "")
                    for plg, v in sorted(res.items())) or "none due"))
                record_run(conn, "pregame", "all", started, pace.n - n0,
                           {"games": sum(v.get("games", 0) for v in res.values())},
                           sum(v.get("errors", 0) for v in res.values()))
                continue
        except Exception as e:
            st = {"errors": 1, "fatal": f"{type(e).__name__}: {e}"}
            log(traceback.format_exc())
        failed |= bool(st.get("errors"))
        record_run(conn, job, lg, started, pace.n - n0, st, st.get("errors", 0), st.get("fatal"))
        chg = changed(st)
        label = lg if job == kind else f"{job} {lg}"
        parts.append(f"{label} +{chg}" + (f" {st['errors']}err" if st.get("errors") else ""))
        log(f"prelog {job} {lg}: {st}")
    return not failed, parts


SOURCE_KINDS = ("sources", "goalies", "nhl-lines", "nbainj", "nflverse")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("kind", choices=("morning", "rosters", "depth", "transactions", "pregame",
                                     "status", "check", "nbainj-backfill", "nbainj-rematch") + SOURCE_KINDS)
    ap.add_argument("--db", default="data/bayesratings.db")
    ap.add_argument("--leagues", default=",".join(LEAGUES))
    ap.add_argument("--rate", type=float, default=1.0, help="min seconds between requests")
    ap.add_argument("--status-file", default=None)
    ap.add_argument("--from", dest="date_from", help="nbainj-backfill: first date (YYYY-MM-DD)")
    ap.add_argument("--to", dest="date_to", help="nbainj-backfill: last date (YYYY-MM-DD)")
    a = ap.parse_args(argv)
    from .backfill import Pacer
    conn = db.connect(a.db)
    conn.isolation_level = None                     # explicit transactions (Tx)
    db.init_db(conn)
    if a.kind == "status":
        print(json.dumps(status(conn), indent=1, sort_keys=True))
        return 0
    stamp = f"{datetime.now(espn.CENTRAL):%Y-%m-%d %H:%M} CT"
    if a.kind == "check":
        from .sources import check
        started = _now()
        res = check.check(conn)
        for src, lvl, msg in res:
            log(f"CHECK {lvl:4} {src}: {msg}")
        line = check.summary_line(res)
        worst = line.split(" CHECK ")[1].split()[0]
        record_run(conn, "check", "all", started, 0, {"results": len(res)},
                   int(worst == "FAIL"), f"{worst} {line.split(' CHECK ')[1].split()[1]}")
        write_status(a.status_file, line)
        log(line)
        return 1 if worst == "FAIL" else 0
    leagues = [x.strip().lower() for x in a.leagues.split(",") if x.strip()]
    pace = Pacer(a.rate)
    t0 = time.monotonic()
    if a.kind == "nbainj-rematch":
        from .sources import nbainj
        st = nbainj.rematch(conn, log=log)
        ok, parts = True, [f"nba injury rematch {st}"]
    elif a.kind == "nbainj-backfill":
        from datetime import date as _date
        from .sources import nbainj
        started = _now()
        st = nbainj.backfill(conn, pace, _date.fromisoformat(a.date_from), _date.fromisoformat(a.date_to))
        record_run(conn, "nba_injury_backfill", "nba", started, pace.n, st, st.get("errors", 0))
        ok, parts = not st.get("errors"), [f"nba injury backfill {st}"]
    elif a.kind in SOURCE_KINDS:
        from .sources import runner
        ok, parts = runner.run_jobs(conn, a.kind, pace)
    else:
        ok, parts = run(conn, a.kind, leagues, pace)
        if a.kind == "pregame":
            # gap-source pre-game jobs; isolated from the ESPN pass above
            try:
                from .sources import runner
                ok2, parts2 = runner.run_jobs(conn, "pregame", pace)
            except Exception as e:
                ok2, parts2 = False, [f"sources ERROR {type(e).__name__}: {e}"]
                log(traceback.format_exc())
            ok, parts = ok and ok2, parts + parts2
    line = (f"{stamp} {'OK' if ok else 'FAIL'} "
            f"prelog {a.kind}: {' | '.join(parts) or 'nothing in season'} | {pace.n} req "
            f"{time.monotonic() - t0:.0f}s || {status_line(conn)}")
    write_status(a.status_file, line)
    log(line)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
