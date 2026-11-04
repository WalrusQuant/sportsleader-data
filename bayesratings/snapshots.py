"""Load published roster, injury, transaction, depth, probable, lineup, and
weather rows, then refresh the ones ESPN still serves.

The copied passes are prelog.rosters_pass, depth_pass, and transactions_pass.
Scoreboard probables and ESPN weather are updated by backfill, which already
calls prelog.log_scoreboard. History already in the release files is loaded
first so a run cannot drop it.

These sources are not re-fetched: NBA official injury PDFs, NFL practice
reports (nflverse), MLB Stats API transactions, Daily Faceoff, and the
National Weather Service. Their existing rows are loaded and written back
unchanged.
"""

import hashlib
import json

import pandas as pd

from . import prelog
from .prelog import DEPTH_FIELDS, INJ_FIELDS, ROSTER_FIELDS

DATASETS = (
    "rosters", "injuries", "transactions", "depth_charts",
    "probables", "lineups", "weather",
)
# Kept on disk, not re-fetched. Named in the run log.
NOT_REFRESHED = (
    "injuries from the NBA official report",
    "injuries from the NFL practice report",
    "transactions from the MLB Stats API",
    "probables and lineups from Daily Faceoff",
    "lineups from NBA.com",
    "weather from the National Weather Service",
)
DETAIL_CAP = 25
SEEN_TABLES = (
    "roster_states", "nfl_depth_states", "game_probables",
    "game_weather", "injury_details",
)


def empty(v):
    if v is None:
        return True
    try:
        return bool(pd.isna(v))
    except (TypeError, ValueError):
        return False


def iso(v):
    if empty(v):
        return None
    if isinstance(v, pd.Timestamp):
        ts = v.tz_convert("UTC") if v.tzinfo else v.tz_localize("UTC")
        return ts.strftime("%Y-%m-%dT%H:%M:%SZ")
    if hasattr(v, "isoformat") and not isinstance(v, str):
        return v.isoformat()
    return str(v)


def as_int(v):
    if empty(v):
        return None
    return int(v)


def text(v):
    if empty(v):
        return None
    if isinstance(v, float) and v == int(v):
        return str(int(v))
    return str(v)


def state_hash(fields, row):
    payload = json.dumps([row.get(f) for f in fields], default=str)
    return hashlib.sha1(payload.encode()).hexdigest()[:16]


def stable_id(*parts):
    raw = "|".join("" if p is None else str(p) for p in parts)
    return int(hashlib.sha1(raw.encode()).hexdigest()[:15], 16)


def remember(conn):
    saved = {}
    for table in SEEN_TABLES:
        saved[table] = conn.execute(
            f"SELECT id, state_hash, valid_from, last_seen FROM {table}").fetchall()
    return saved


def restore(conn, saved):
    """Put last_seen back when a pass only touched the clock, and put names
    back after the player extract drops anyone who has no box-score row."""
    for table, rows in saved.items():
        conn.executemany(
            f"UPDATE {table} SET last_seen = ? WHERE id = ? AND state_hash = ? AND valid_from = ?",
            [(last, rid, digest, start) for rid, digest, start, last in rows])
    conn.execute(
        "INSERT INTO players (league, athlete_id, name) "
        "SELECT league, athlete_id, MAX(name) FROM roster_states "
        "WHERE name IS NOT NULL GROUP BY league, athlete_id "
        "ON CONFLICT (league, athlete_id) DO UPDATE SET "
        "name = COALESCE(players.name, excluded.name)")
    conn.commit()


def _player(conn, league, athlete_id, name, position):
    """Remember a name. Do not fill position: NFL logs publish a null position,
    and a roster position would change every one of those rows."""
    if athlete_id is None:
        return
    conn.execute(
        "INSERT INTO players (league, athlete_id, name) VALUES (?,?,?) "
        "ON CONFLICT (league, athlete_id) DO UPDATE SET "
        "name = COALESCE(players.name, excluded.name)",
        (league, int(athlete_id), name))


def load(conn, teams, league, dataset, frame):
    if dataset == "rosters":
        return _rosters(conn, teams, league, frame)
    if dataset == "injuries":
        return _injuries(conn, teams, league, frame)
    if dataset == "transactions":
        return _transactions(conn, teams, league, frame)
    if dataset == "depth_charts":
        return _depth(conn, teams, league, frame)
    if dataset == "probables":
        return _probables(conn, teams, league, frame)
    if dataset == "lineups":
        return _lineups(conn, teams, league, frame)
    if dataset == "weather":
        return _weather(conn, teams, league, frame)
    return 0


def _rosters(conn, teams, league, frame):
    rows = []
    for rec in frame.to_dict("records"):
        tid = teams.id(league, rec["team_id"], rec.get("team_abbr"), rec.get("team_name"))
        row = {
            "league": league, "team_id": tid, "athlete_id": int(rec["player_id"]),
            "name": text(rec.get("player_name")), "position": text(rec.get("position")),
            "jersey": text(rec.get("jersey")), "roster_group": text(rec.get("roster_group")),
            "status": text(rec.get("status")), "injury_status": text(rec.get("injury_status")),
            "injury_date": iso(rec.get("injury_date")),
            "valid_from": iso(rec.get("valid_from")), "valid_to": iso(rec.get("valid_to")),
            "last_seen": iso(rec.get("last_seen")),
        }
        row["state_hash"] = state_hash(ROSTER_FIELDS, row)
        rows.append(row)
        _player(conn, league, row["athlete_id"], row["name"], row["position"])
    conn.executemany(
        "INSERT INTO roster_states (league, team_id, athlete_id, name, position, jersey, "
        "roster_group, status, injury_status, injury_date, raw_json, state_hash, "
        "valid_from, valid_to, last_seen) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [(r["league"], r["team_id"], r["athlete_id"], r["name"], r["position"], r["jersey"],
          r["roster_group"], r["status"], r["injury_status"], r["injury_date"], "{}",
          r["state_hash"], r["valid_from"], r["valid_to"], r["last_seen"]) for r in rows])
    conn.commit()
    return len(rows)


def _injuries(conn, teams, league, frame):
    n = 0
    espn, nba, nfl = [], [], []
    for rec in frame.to_dict("records"):
        tid = None if empty(rec.get("team_id")) else teams.id(
            league, rec["team_id"], rec.get("team_abbr"), rec.get("team_name"))
        aid = as_int(rec.get("player_id"))
        _player(conn, league, aid, text(rec.get("player_name")), text(rec.get("position")))
        src = rec.get("source")
        if src == "espn":
            if aid is None:
                continue
            row = {
                "league": league, "team_id": tid, "athlete_id": aid,
                "status": text(rec.get("status")), "injury_date": iso(rec.get("reported_at")),
                "type": text(rec.get("injury")), "detail": text(rec.get("detail")),
                "side": text(rec.get("side")), "return_date": iso(rec.get("return_date")),
                "fantasy_status": None, "short_comment": text(rec.get("comment")),
            }
            row["injury_id"] = stable_id("inj", league, aid, row["injury_date"], row["status"],
                                         row["type"], iso(rec.get("valid_from")))
            row["state_hash"] = state_hash(INJ_FIELDS, row)
            espn.append((row, iso(rec.get("valid_from")), iso(rec.get("valid_to")),
                         iso(rec.get("last_seen"))))
        elif src == "nba_official_report":
            nba.append((
                league, iso(rec.get("game_date")) or (iso(rec.get("valid_from")) or "")[:10],
                text(rec.get("team_name")) or "",
                text(rec.get("player_name")) or "",
                tid, as_int(rec.get("game_id")), aid, text(rec.get("player_name")),
                text(rec.get("status")), text(rec.get("injury")),
                state_hash(("status", "reason"), {"status": text(rec.get("status")),
                                                   "reason": text(rec.get("injury"))}),
                iso(rec.get("valid_from")), iso(rec.get("valid_to")), iso(rec.get("last_seen")),
            ))
        elif src == "nfl_practice_report":
            nfl.append((
                league, int(rec["season"]), as_int(rec.get("week")) or 0,
                text(rec.get("team_abbr")) or "",
                f"loaded-{aid}-{iso(rec.get('valid_from'))}",
                tid, aid, text(rec.get("player_name")), text(rec.get("position")),
                text(rec.get("injury")), text(rec.get("detail")), text(rec.get("status")),
                text(rec.get("practice_status")),
                state_hash(("report_status",), {"report_status": text(rec.get("status"))}),
                iso(rec.get("valid_from")), iso(rec.get("valid_to")), iso(rec.get("last_seen")),
            ))
    if espn:
        conn.executemany(
            "INSERT INTO injury_details (league, injury_id, team_id, athlete_id, status, "
            "injury_date, type, detail, side, return_date, fantasy_status, short_comment, "
            "raw_json, state_hash, valid_from, valid_to, last_seen) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [(r["league"], r["injury_id"], r["team_id"], r["athlete_id"], r["status"],
              r["injury_date"], r["type"], r["detail"], r["side"], r["return_date"],
              r["fantasy_status"], r["short_comment"], "{}", r["state_hash"], vf, vt, ls)
             for r, vf, vt, ls in espn])
        n += len(espn)
    if nba:
        conn.executemany(
            "INSERT INTO nba_injury_states (league, game_date, team_name, player_key, team_id, "
            "espn_event_id, athlete_id, player_name, status, reason, raw_json, state_hash, "
            "valid_from, valid_to, last_seen) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [(a, b, c, d, e, f, g, h, i, j, "{}", k, l, m, n_) for a, b, c, d, e, f, g, h, i, j, k, l, m, n_ in nba])
        n += len(nba)
    if nfl:
        conn.executemany(
            "INSERT INTO nfl_injury_report_states (league, season, week, team, gsis_id, team_id, "
            "athlete_id, full_name, position, report_primary_injury, report_secondary_injury, "
            "report_status, practice_status, raw_json, state_hash, valid_from, valid_to, last_seen) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [(a, b, c, d, e, f, g, h, i, j, k, l, m, "{}", n_, o, p, q)
             for a, b, c, d, e, f, g, h, i, j, k, l, m, n_, o, p, q in nfl])
        n += len(nfl)
    conn.commit()
    return n


def _transactions(conn, teams, league, frame):
    espn, mlb = [], []
    sigs = []
    for rec in frame.to_dict("records"):
        desc = text(rec.get("description")) or ""
        when = iso(rec.get("date"))
        day = (when or "")[:10]
        espn_team = as_int(rec.get("team_id"))
        tid = None if espn_team is None else teams.id(
            league, espn_team, rec.get("team_abbr"), rec.get("team_name"))
        seen = iso(rec.get("first_seen"))
        if rec.get("source") == "mlb":
            aid = as_int(rec.get("player_id"))
            _player(conn, "mlb", aid, text(rec.get("player_name")), None)
            mlb.append((
                stable_id("mlb-tx", day, espn_team, desc, seen),
                day, text(rec.get("type")), desc, aid, tid, seen, seen,
            ))
        else:
            key = hashlib.sha1(f"{league}|{day}|{espn_team}|{desc}".encode()).hexdigest()[:20]
            espn.append((league, key, day, espn_team, tid, desc, seen))
            sigs.append((league, day, espn_team, desc, key))
    if espn:
        conn.executemany(
            "INSERT OR IGNORE INTO transactions (league, txn_key, txn_date, espn_team_id, "
            "team_id, description, raw_json, first_seen) VALUES (?,?,?,?,?,?,?,?)",
            [(a, b, c, d, e, f, "{}", g) for a, b, c, d, e, f, g in espn])
    if mlb:
        conn.executemany(
            "INSERT OR IGNORE INTO mlb_transactions (txn_id, txn_date, type_desc, description, "
            "athlete_id, team_id, first_seen, last_seen, il_related) VALUES (?,?,?,?,?,?,?,?,0)",
            mlb)
    conn.commit()
    frame.attrs["tx_sigs"] = sigs
    _transactions.sigs = getattr(_transactions, "sigs", []) + sigs
    return len(espn) + len(mlb)


def _dedupe_transactions(conn):
    """Drop ESPN rows the pass re-inserted, and rows older than this season's file."""
    sigs = getattr(_transactions, "sigs", [])
    known = {(lg, day, team, desc) for lg, day, team, desc, _key in sigs}
    owned = {(lg, key) for lg, _day, _team, _desc, key in sigs}
    earliest = {}
    for lg, day, _team, _desc, _key in sigs:
        earliest[lg] = day if lg not in earliest else min(earliest[lg], day)
    drop = []
    for lg, key, txn_date, espn_team, desc in conn.execute(
            "SELECT league, txn_key, txn_date, espn_team_id, description FROM transactions"):
        day = str(txn_date or "")[:10]
        sig = (lg, day, espn_team, desc)
        if (lg, key) in owned:
            continue
        if sig in known or (lg in earliest and day < earliest[lg]):
            drop.append((lg, key))
    conn.executemany("DELETE FROM transactions WHERE league = ? AND txn_key = ?", drop)
    conn.commit()
    return len(drop)


def _depth(conn, teams, league, frame):
    rows = []
    for rec in frame.to_dict("records"):
        tid = teams.id(league, rec["team_id"], rec.get("team_abbr"), rec.get("team_name"))
        row = {
            "league": league, "team_id": tid, "formation": text(rec.get("formation")) or "",
            "slot": text(rec.get("slot")) or "", "depth": as_int(rec.get("depth")) or 0,
            "athlete_id": as_int(rec.get("player_id")), "name": text(rec.get("player_name")),
            "position": text(rec.get("position")),
        }
        row["state_hash"] = state_hash(DEPTH_FIELDS, row)
        rows.append((row, iso(rec.get("valid_from")), iso(rec.get("valid_to")),
                     iso(rec.get("last_seen"))))
    conn.executemany(
        "INSERT INTO nfl_depth_states (league, team_id, formation, slot, depth, athlete_id, "
        "name, position, raw_json, state_hash, valid_from, valid_to, last_seen) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [(r["league"], r["team_id"], r["formation"], r["slot"], r["depth"], r["athlete_id"],
          r["name"], r["position"], "{}", r["state_hash"], vf, vt, ls) for r, vf, vt, ls in rows])
    conn.commit()
    return len(rows)


def _kind(role):
    if role == "starting_pitcher":
        return "SP"
    if role == "starting_goalie":
        return "SG"
    return role or "?"


def _probables(conn, teams, league, frame):
    espn, dfo = [], []
    for rec in frame.to_dict("records"):
        tid = None if empty(rec.get("team_id")) else teams.id(
            league, rec["team_id"], rec.get("team_abbr"), rec.get("team_name"))
        aid = as_int(rec.get("player_id"))
        if rec.get("source") == "daily_faceoff":
            dfo.append((
                league, iso(rec.get("game_date")) or "", int(rec["team_id"]),
                tid, as_int(rec.get("game_id")), text(rec.get("player_name")), aid,
                text(rec.get("status")), text(rec.get("note")),
                state_hash(("athlete_id", "strength"), {"athlete_id": aid,
                                                        "strength": text(rec.get("status"))}),
                iso(rec.get("valid_from")), iso(rec.get("valid_to")), iso(rec.get("last_seen")),
            ))
        else:
            row = {
                "espn_event_id": as_int(rec.get("game_id")), "league": league, "team_id": tid,
                "kind": _kind(text(rec.get("role"))), "athlete_id": aid,
                "name": text(rec.get("player_name")), "status": text(rec.get("status")),
            }
            row["state_hash"] = state_hash(("athlete_id", "name", "status"), row)
            espn.append((row, iso(rec.get("valid_from")), iso(rec.get("valid_to")),
                         iso(rec.get("last_seen"))))
    if espn:
        conn.executemany(
            "INSERT INTO game_probables (espn_event_id, league, team_id, kind, athlete_id, "
            "name, status, raw_json, state_hash, valid_from, valid_to, last_seen) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            [(r["espn_event_id"], r["league"], r["team_id"], r["kind"], r["athlete_id"],
              r["name"], r["status"], "{}", r["state_hash"], vf, vt, ls)
             for r, vf, vt, ls in espn])
    if dfo:
        conn.executemany(
            "INSERT INTO nhl_goalie_states (league, game_date, dfo_team_id, team_id, "
            "espn_event_id, goalie_name, athlete_id, strength, news_details, raw_json, "
            "state_hash, valid_from, valid_to, last_seen) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [(a, b, c, d, e, f, g, h, i, "{}", j, k, l, m) for a, b, c, d, e, f, g, h, i, j, k, l, m in dfo])
    conn.commit()
    return len(espn) + len(dfo)


def _lineups(conn, teams, league, frame):
    mlb, nba, nhl, pre = [], [], [], []
    for rec in frame.to_dict("records"):
        tid = None if empty(rec.get("team_id")) else teams.id(
            league, rec["team_id"], rec.get("team_abbr"), rec.get("team_name"))
        aid = as_int(rec.get("player_id"))
        src = rec.get("source")
        vf, vt, ls = iso(rec.get("valid_from")), iso(rec.get("valid_to")), iso(rec.get("last_seen"))
        if src == "mlb":
            role = text(rec.get("role"))
            slot = 0 if role == "starting_pitcher" else (as_int(rec.get("slot")) or 0)
            mlb.append((
                league, stable_id("mlb-line", rec.get("game_id"), tid, slot, aid, vf),
                as_int(rec.get("game_id")), "home", slot, tid, aid,
                text(rec.get("player_name")), text(rec.get("position")),
                state_hash(("athlete_id", "position"), {"athlete_id": aid,
                                                        "position": text(rec.get("position"))}),
                vf, vt, ls,
            ))
        elif src in ("nba", "espn_depth"):
            nba.append((
                league, as_int(rec.get("game_id")), tid, text(rec.get("slot")) or "",
                aid, text(rec.get("player_name")), text(rec.get("status")),
                "nba.com" if src == "nba" else "espn_depth",
                state_hash(("athlete_id", "status"), {"athlete_id": aid,
                                                      "status": text(rec.get("status"))}),
                vf, vt, ls,
            ))
        elif src == "daily_faceoff":
            role = text(rec.get("role")) or ""
            category = role[5:] if role.startswith("line_") else role
            nhl.append((
                league, int(rec["team_id"]) if not empty(rec.get("team_id")) else 0,
                tid, text(rec.get("slot")) or "", category, aid or 0, aid,
                text(rec.get("player_name")), text(rec.get("position")), text(rec.get("status")),
                state_hash(("athlete_id",), {"athlete_id": aid}), vf, vt, ls,
            ))
        elif src == "espn_pregame":
            pre.append((
                as_int(rec.get("game_id")), vf or "", tid, text(rec.get("role")) or "",
                aid or 0, text(rec.get("player_name")), as_int(rec.get("slot")),
                text(rec.get("position")), text(rec.get("status")),
            ))
    if mlb:
        conn.executemany(
            "INSERT INTO mlb_lineup_states (league, game_pk, espn_event_id, side, slot, team_id, "
            "athlete_id, name, position, raw_json, state_hash, valid_from, valid_to, last_seen) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [(a, b, c, d, e, f, g, h, i, "{}", j, k, l, m) for a, b, c, d, e, f, g, h, i, j, k, l, m in mlb])
    if nba:
        conn.executemany(
            "INSERT INTO nba_lineup_states (league, espn_event_id, team_id, slot, athlete_id, "
            "name, status, src, raw_json, state_hash, valid_from, valid_to, last_seen) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [(a, b, c, d, e, f, g, h, "{}", i, j, k, l) for a, b, c, d, e, f, g, h, i, j, k, l in nba])
    if nhl:
        conn.executemany(
            "INSERT INTO nhl_line_states (league, dfo_team_id, team_id, group_id, category, "
            "dfo_player_id, athlete_id, name, position_id, injury_status, raw_json, state_hash, "
            "valid_from, valid_to, last_seen) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [(a, b, c, d, e, f, g, h, i, j, "{}", k, l, m, n) for a, b, c, d, e, f, g, h, i, j, k, l, m, n in nhl])
    if pre:
        conn.executemany(
            "INSERT OR IGNORE INTO pregame_lineups (espn_event_id, fetched_at, team_id, role, "
            "athlete_id, name, order_no, position, status) VALUES (?,?,?,?,?,?,?,?,?)",
            pre)
    conn.commit()
    return len(mlb) + len(nba) + len(nhl) + len(pre)


def _weather(conn, teams, league, frame):
    espn, nws = [], []
    for rec in frame.to_dict("records"):
        src = text(rec.get("source")) or ""
        eid = as_int(rec.get("game_id"))
        if src == "nws" or src == "open-meteo":
            nws.append((
                eid, iso(rec.get("valid_from")) or "", text(rec.get("fetch_phase")) or "morning",
                "open-meteo" if str(rec.get("condition") or "").startswith("WMO") else "nws",
                as_int(rec.get("hour_offset")) or 0, league,
                iso(rec.get("period_start")), rec.get("temperature_f") if not empty(rec.get("temperature_f")) else None,
                rec.get("wind_mph") if not empty(rec.get("wind_mph")) else None,
                rec.get("wind_gust_mph") if not empty(rec.get("wind_gust_mph")) else None,
                text(rec.get("wind_dir")),
                rec.get("humidity_pct") if not empty(rec.get("humidity_pct")) else None,
                rec.get("precip_pct") if not empty(rec.get("precip_pct")) else None,
                rec.get("dewpoint_f") if not empty(rec.get("dewpoint_f")) else None,
                text(rec.get("condition")),
            ))
        elif src.startswith("espn_"):
            row = {
                "espn_event_id": eid, "league": league, "source": src[len("espn_"):],
                "condition": text(rec.get("condition")), "condition_id": None,
                "temperature": as_int(rec.get("temperature_f")),
                "high_temp": as_int(rec.get("high_f")), "low_temp": as_int(rec.get("low_f")),
                "gust": as_int(rec.get("wind_gust_mph")),
                "precipitation": as_int(rec.get("precip_pct")),
            }
            row["state_hash"] = state_hash(
                ("condition", "condition_id", "temperature", "high_temp", "low_temp", "gust", "precipitation"),
                row)
            espn.append((row, iso(rec.get("valid_from")), iso(rec.get("valid_to")),
                         iso(rec.get("last_seen")) or iso(rec.get("valid_from"))))
    # Touch team ids so home and away abbreviations survive the games join.
    for rec in frame.to_dict("records"):
        if not empty(rec.get("home_team_id")):
            teams.id(league, rec["home_team_id"], rec.get("home_team_abbr"), None)
        if not empty(rec.get("away_team_id")):
            teams.id(league, rec["away_team_id"], rec.get("away_team_abbr"), None)
    if espn:
        conn.executemany(
            "INSERT INTO game_weather (espn_event_id, league, source, condition, condition_id, "
            "temperature, high_temp, low_temp, gust, precipitation, raw_json, state_hash, "
            "valid_from, valid_to, last_seen) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [(r["espn_event_id"], r["league"], r["source"], r["condition"], r["condition_id"],
              r["temperature"], r["high_temp"], r["low_temp"], r["gust"], r["precipitation"],
              "{}", r["state_hash"], vf, vt, ls) for r, vf, vt, ls in espn])
    if nws:
        conn.executemany(
            "INSERT OR IGNORE INTO game_weather_fcst (espn_event_id, fetched_at, phase, provider, "
            "hour_offset, league, period_start, temp_f, wind_mph, wind_gust_mph, wind_dir, "
            "humidity, precip_pct, dewpoint_f, short_forecast) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            nws)
    # Weather for a game that is not in the games file still has to join.
    # STATUS_REMOVED keeps the stub out of the games export.
    have = {r[0] for r in conn.execute("SELECT espn_event_id FROM games")}
    stubs = []
    seen = set()
    for rec in frame.to_dict("records"):
        eid = as_int(rec.get("game_id"))
        if eid is None or eid in have or eid in seen:
            continue
        if empty(rec.get("home_team_id")) or empty(rec.get("away_team_id")):
            continue
        seen.add(eid)
        day = iso(rec.get("game_date")) or "2026-01-01"
        stubs.append((
            eid, league, int(rec["season"]), 2, f"{day[:10]}T12:00:00Z", day[:10],
            teams.id(league, rec["home_team_id"], rec.get("home_team_abbr"), None),
            teams.id(league, rec["away_team_id"], rec.get("away_team_abbr"), None),
            "STATUS_REMOVED", text(rec.get("venue")),
        ))
    if stubs:
        conn.executemany(
            "INSERT OR IGNORE INTO games (espn_event_id, league, season, season_type, "
            "start_time_utc, game_date, home_team_id, away_team_id, completed, status, "
            "venue, neutral_site, covid_2020, if_necessary, series_completed) "
            "VALUES (?,?,?,?,?,?,?,?,0,?,?,0,0,0,0)",
            stubs)
    conn.commit()
    return len(espn) + len(nws)


def seed_lookups(conn):
    """Skip injury-detail fetches for roster injuries already published."""
    conn.execute(
        "INSERT OR IGNORE INTO injury_lookups (league, athlete_id, roster_status, "
        "roster_injury_date, injury_id, checked_at) "
        "SELECT league, athlete_id, injury_status, injury_date, NULL, last_seen "
        "FROM roster_states WHERE valid_to IS NULL AND injury_status IS NOT NULL")
    conn.commit()


def refresh(conn, log=print):
    """Current roster, depth chart, and the newest ESPN transactions."""
    from .backfill import Pacer
    pace = Pacer(0.0)
    out = {}
    for league in ("mlb", "nba", "nfl", "nhl"):
        out[f"roster_{league}"] = prelog.rosters_pass(
            conn, league, pace, log=log, detail_cap=DETAIL_CAP)
        out[f"tx_{league}"] = prelog.transactions_pass(conn, league, pace, log=log)
    out["depth"] = prelog.depth_pass(conn, pace, log=log)
    out["tx_dupes_removed"] = _dedupe_transactions(conn)
    out["requests"] = pace.n
    return out
