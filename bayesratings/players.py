"""Player-level box lines from the stored ESPN game summaries.

    python -m bayesratings.players --db data/bayesratings.db [--leagues mlb,nhl]
        [--limit N] [--full] [--validate]

Reads game_summaries.json_gz (never calls ESPN) for completed, playable,
non-exhibition games and writes one row per player per game per category
into the player_game_* tables (see schema.sql), plus players and
player_team_seasons. Idempotent and resumable: each game's rows are replaced
in one transaction and logged in player_extract_log; a game is redone only
when its summary was refetched, its log isn't "ok", or PARSER_VERSION grew.
Counts of skipped and unparseable games are always logged, never silent.
Data only: nothing here feeds the model.
"""

import argparse
import gzip
import json
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone

from . import db

PARSER_VERSION = 1
LEAGUES = ("mlb", "nba", "nfl", "nhl")

TABLES = {
    "mlb": ("player_game_mlb_batting", "player_game_mlb_pitching"),
    "nba": ("player_game_nba",),
    "nhl": ("player_game_nhl_skater", "player_game_nhl_goalie"),
    "nfl": ("player_game_nfl_passing", "player_game_nfl_rushing",
            "player_game_nfl_receiving", "player_game_nfl_other"),
}
COMMON = ("espn_event_id", "season", "season_type", "game_date", "team_id",
          "opp_team_id", "is_home", "athlete_id")


class BoxError(ValueError):
    """A summary whose player box can't be read (logged as status=error)."""


def _num(v, cast=int):
    """ESPN display text -> number; None for '', '--', '-' and the like."""
    if v is None:
        return None
    s = str(v).strip().replace(",", "")
    if s in ("", "-", "--", "---") or s.lower() == "nan":
        return None
    try:
        x = float(s)
    except ValueError:
        return None
    if cast is int:
        return int(x) if x.is_integer() else x
    return cast(x)


def _secs(v):
    """'19:46' -> 1186 seconds (mm:ss); a plain number is minutes."""
    if v is None or str(v).strip() in ("", "-", "--"):
        return None
    s = str(v).strip()
    if ":" in s:
        m, _, sec = s.partition(":")
        try:
            return int(m) * 60 + int(sec)
        except ValueError:
            return None
    n = _num(s, float)
    return None if n is None else int(round(n * 60))


def _outs(ip):
    """Innings pitched '5.2' (5 innings + 2 outs) -> 17 outs."""
    if ip is None or str(ip).strip() in ("", "-", "--"):
        return None
    whole, _, part = str(ip).strip().partition(".")
    try:
        p = int(part or 0)
        if p > 2:
            return None
        return int(whole) * 3 + p
    except ValueError:
        return None


def flatten(keys, stats):
    """Pair ESPN's `keys` with one athlete's `stats` -> (raw, flat). raw is
    {key: text} as ESPN gives it (kept as stats_json); flat also splits
    compound keys ('hits-atBats' '1-4', 'completions/passingAttempts'
    '22/32') into their parts. Raises BoxError if the lengths differ (a
    format we don't understand)."""
    if len(keys) != len(stats):
        raise BoxError(f"{len(keys)} keys vs {len(stats)} stats")
    raw = {k: "" if v is None else str(v) for k, v in zip(keys, stats)}
    flat = dict(raw)
    for k, v in raw.items():
        for sep in ("-", "/"):
            if k.count(sep) == 1 and "." not in k:
                a, b = k.split(sep)
                parts = v.split(sep) if not v.startswith("-") else []
                if len(parts) == 2:
                    flat.setdefault(a, parts[0])
                    flat.setdefault(b, parts[1])
    return raw, flat


def _lines(group):
    """(box order, athlete entry, athlete id, raw, flat) per player. Lines
    without an athlete id (ESPN placeholders such as an empty pinch-hitter
    slot) are dropped here; parse_game counts them."""
    keys = group.get("keys") or []
    i = 0
    for a in group.get("athletes") or []:
        ath = a.get("athlete") or {}
        if ath.get("id") in (None, ""):
            continue
        i += 1
        st = a.get("stats") or []
        raw, flat = flatten(keys, st) if st else ({}, {})
        yield i, a, int(ath["id"]), raw, flat


def _pos(entry):
    """Position abbreviation from a box line (game position first)."""
    for p in (entry.get("position"), (entry.get("athlete") or {}).get("position")):
        if isinstance(p, dict) and p.get("abbreviation"):
            return p["abbreviation"]
    return None


def _decision(entry):
    for n in entry.get("notes") or []:
        if n.get("type") == "pitchingDecision" and n.get("text"):
            return n["text"].split(",")[0].strip() or None
    return None


def _mlb(team, base):
    out = defaultdict(list)
    for g in team.get("statistics") or []:
        keys = g.get("keys") or []
        if "hits-atBats" in keys:
            for i, a, aid, raw, f in _lines(g):
                out["player_game_mlb_batting"].append(dict(
                    base, athlete_id=aid, position=_pos(a), starter=int(bool(a.get("starter"))),
                    bat_order=a.get("batOrder"), box_order=i, ab=_num(f.get("atBats")),
                    r=_num(f.get("runs")), h=_num(f.get("hits")), rbi=_num(f.get("RBIs")),
                    hr=_num(f.get("homeRuns")), bb=_num(f.get("walks")), k=_num(f.get("strikeouts")),
                    pitches=_num(f.get("pitches")), stats_json=raw, _ath=a))
        elif "fullInnings.partInnings" in keys:
            for i, a, aid, raw, f in _lines(g):
                out["player_game_mlb_pitching"].append(dict(
                    base, athlete_id=aid, position=_pos(a), starter=int(i == 1), pitch_order=i,
                    decision=_decision(a), outs=_outs(f.get("fullInnings.partInnings")),
                    h=_num(f.get("hits")), r=_num(f.get("runs")), er=_num(f.get("earnedRuns")),
                    bb=_num(f.get("walks")), k=_num(f.get("strikeouts")), hr=_num(f.get("homeRuns")),
                    pitches=_num(f.get("pitches")), strikes=_num(f.get("strikes")),
                    stats_json=raw, _ath=a, _flag=(i == 1) != bool(a.get("starter"))))
        elif g.get("athletes"):
            raise BoxError(f"unknown MLB group keys {keys[:3]}")
    return out


_NBA = {"minutes": "minutes", "points": "pts", "fieldGoalsMade": "fgm",
        "fieldGoalsAttempted": "fga", "threePointFieldGoalsMade": "fg3m",
        "threePointFieldGoalsAttempted": "fg3a", "freeThrowsMade": "ftm",
        "freeThrowsAttempted": "fta", "rebounds": "reb", "offensiveRebounds": "oreb",
        "defensiveRebounds": "dreb", "assists": "ast", "turnovers": "tov",
        "steals": "stl", "blocks": "blk", "fouls": "pf", "plusMinus": "plus_minus"}


def _nba(team, base):
    out = defaultdict(list)
    for g in team.get("statistics") or []:
        keys = g.get("keys") or []
        if g.get("athletes") and "points" not in keys:
            raise BoxError(f"unknown NBA group keys {keys[:3]}")
        for i, a, aid, raw, f in _lines(g):
            dnp = bool(a.get("didNotPlay"))
            row = dict(base, athlete_id=aid, position=_pos(a), starter=int(bool(a.get("starter"))),
                       did_not_play=int(dnp), dnp_reason=(a.get("reason") or None) if dnp else None,
                       ejected=int(bool(a.get("ejected"))), stats_json=raw, _ath=a)
            for k, col in _NBA.items():
                row[col] = _num(f.get(k))
            out["player_game_nba"].append(row)
    return out


_SKATER = {"timeOnIce": "toi_sec", "powerPlayTimeOnIce": "pp_toi_sec",
           "shortHandedTimeOnIce": "sh_toi_sec", "evenStrengthTimeOnIce": "es_toi_sec"}
_SKATER_N = {"shifts": "shifts", "goals": "g", "assists": "a", "shotsTotal": "shots",
             "shotsMissed": "shots_missed", "plusMinus": "plus_minus", "hits": "hits",
             "blockedShots": "blocks", "takeaways": "takeaways", "giveaways": "giveaways",
             "faceoffsWon": "fo_won", "faceoffsLost": "fo_lost", "penaltyMinutes": "pim",
             "shootoutGoals": "so_goals"}
_GOALIE_N = {"shotsAgainst": "sa", "goalsAgainst": "ga", "saves": "sv",
             "evenStrengthSaves": "es_sv", "powerPlaySaves": "pp_sv",
             "shortHandedSaves": "sh_sv", "shootoutShotsAgainst": "so_sa",
             "shootoutSaves": "so_sv", "penaltyMinutes": "pim"}


def _nhl(team, base):
    out = defaultdict(list)
    for g in team.get("statistics") or []:
        name = g.get("name")
        if name == "goalies":
            rows = []
            for i, a, aid, raw, f in _lines(g):
                row = dict(base, athlete_id=aid, position=_pos(a), goalie_order=i,
                           toi_sec=_secs(f.get("timeOnIce")), sv_pct=_num(f.get("savePct"), float),
                           stats_json=raw, _ath=a)
                for k, col in _GOALIE_N.items():
                    row[col] = _num(f.get(k))
                rows.append(row)
            top = max(rows, key=lambda r: (r["toi_sec"] or 0, -r["goalie_order"]), default=None)
            for r in rows:
                r["starter"] = int(r is top)
            out["player_game_nhl_goalie"] += rows
        elif name in ("forwards", "defenses", "skaters"):
            for i, a, aid, raw, f in _lines(g):
                row = dict(base, athlete_id=aid, position=_pos(a), unit=name, stats_json=raw, _ath=a)
                for k, col in _SKATER.items():
                    row[col] = _secs(f.get(k))
                for k, col in _SKATER_N.items():
                    row[col] = _num(f.get(k))
                out["player_game_nhl_skater"].append(row)
        elif g.get("athletes"):
            raise BoxError(f"unknown NHL group {name}")
    return out


def _nfl(team, base):
    out = defaultdict(list)
    for g in team.get("statistics") or []:
        cat = g.get("name")
        for i, a, aid, raw, f in _lines(g):
            b = dict(base, athlete_id=aid, stats_json=raw, _ath=a)
            if cat == "passing":
                out["player_game_nfl_passing"].append(dict(
                    b, cmp=_num(f.get("completions")), att=_num(f.get("passingAttempts")),
                    yds=_num(f.get("passingYards")), td=_num(f.get("passingTouchdowns")),
                    ints=_num(f.get("interceptions")), sacks=_num(f.get("sacks")),
                    sack_yds=_num(f.get("sackYardsLost")), qbr=_num(f.get("adjQBR"), float),
                    rating=_num(f.get("QBRating"), float), _i=i))
            elif cat == "rushing":
                out["player_game_nfl_rushing"].append(dict(
                    b, car=_num(f.get("rushingAttempts")), yds=_num(f.get("rushingYards")),
                    td=_num(f.get("rushingTouchdowns")), long=_num(f.get("longRushing"))))
            elif cat == "receiving":
                out["player_game_nfl_receiving"].append(dict(
                    b, rec=_num(f.get("receptions")), tgt=_num(f.get("receivingTargets")),
                    yds=_num(f.get("receivingYards")), td=_num(f.get("receivingTouchdowns")),
                    long=_num(f.get("longReception"))))
            elif cat:
                out["player_game_nfl_other"].append(dict(b, category=cat))
            else:
                raise BoxError("NFL group without a name")
    qbs = out["player_game_nfl_passing"]
    top = max(qbs, key=lambda r: (r["att"] or 0, -r["_i"]), default=None)
    for r in qbs:
        r["qb_starter"] = int(r is top)
        del r["_i"]
    return out


PARSERS = {"mlb": _mlb, "nba": _nba, "nhl": _nhl, "nfl": _nfl}


def parse_game(summary, game, espn_to_team):
    """({table: [row]}, lines dropped for having no athlete id) for one game.
    `game` is the games row (dict); `espn_to_team` maps ESPN team id -> our
    team_id. Raises BoxError on a box we can't read; rows are {} when the
    summary has no player box."""
    lg = game["league"]
    teams = (summary.get("boxscore") or {}).get("players") or []
    if not teams:
        return {}, 0
    no_id = sum(1 for t in teams for g in t.get("statistics") or []
                for a in g.get("athletes") or []
                if (a.get("athlete") or {}).get("id") in (None, ""))
    if len(teams) != 2:
        raise BoxError(f"{len(teams)} teams in player box")
    out = defaultdict(list)
    seen = set()
    for t in teams:
        try:
            tid = espn_to_team[int(t["team"]["id"])]
        except (KeyError, TypeError, ValueError):
            raise BoxError(f"unknown team {((t.get('team') or {}).get('id'))}")
        if tid not in (game["home_team_id"], game["away_team_id"]):
            raise BoxError(f"box team {tid} not in game")
        home = tid == game["home_team_id"]
        base = dict(espn_event_id=game["espn_event_id"], season=game["season"],
                    season_type=game["season_type"], game_date=game["game_date"],
                    team_id=tid, is_home=int(home),
                    opp_team_id=game["away_team_id"] if home else game["home_team_id"])
        for table, rows in PARSERS[lg](t, base).items():
            for r in rows:
                key = (table, tid, r["athlete_id"], r.get("category"))
                if key in seen:
                    raise BoxError(f"athlete {r['athlete_id']} twice in {table} for one team")
                seen.add(key)
            out[table] += rows
    return out, no_id


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _columns(conn, table):
    return [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]


def store_game(conn, league, eid, rows, cols):
    """Replace one game's rows in every table of its league (no commit)."""
    n = 0
    for table in TABLES[league]:
        conn.execute(f"DELETE FROM {table} WHERE espn_event_id = ?", (eid,))
        data = rows.get(table) or []
        if not data:
            continue
        c = cols[table]
        conn.executemany(
            f"INSERT INTO {table} ({', '.join(c)}) VALUES ({', '.join('?' * len(c))})",
            [tuple(json.dumps(r[k], separators=(",", ":")) if k == "stats_json" else r.get(k)
                   for k in c) for r in data])
        n += len(data)
    return n


def _player_rows(rows):
    """{athlete_id: (name, short_name, primary position, game_date)} for one
    game's lines."""
    out = {}
    for data in rows.values():
        for r in data:
            a = r["_ath"].get("athlete") or {}
            pos = a.get("position")
            pos = pos.get("abbreviation") if isinstance(pos, dict) else None
            prev = out.get(r["athlete_id"])
            out[r["athlete_id"]] = (a.get("displayName") or (prev and prev[0]),
                                    a.get("shortName") or (prev and prev[1]),
                                    pos or (prev and prev[2]), r["game_date"])
    return out


def refresh_players(conn, league):
    """Rebuild players and player_team_seasons for one league from the
    player_game tables (cheap: one GROUP BY per table)."""
    union = " UNION ALL ".join(
        f"SELECT espn_event_id, athlete_id, team_id, season, game_date FROM {t}"
        for t in TABLES[league])
    conn.commit()
    conn.execute("BEGIN IMMEDIATE")
    conn.execute("DELETE FROM player_team_seasons WHERE league = ?", (league,))
    conn.execute(
        "INSERT INTO player_team_seasons (league, athlete_id, season, team_id, games, "
        "first_game_date, last_game_date) SELECT ?, athlete_id, season, team_id, "
        f"COUNT(DISTINCT espn_event_id), MIN(game_date), MAX(game_date) FROM ({union}) "
        "GROUP BY athlete_id, season, team_id", (league,))
    conn.execute("UPDATE players SET games = NULL WHERE league = ?", (league,))
    conn.execute(
        "INSERT INTO players (league, athlete_id, team_id, first_game_date, last_game_date, games) "
        "SELECT ?, athlete_id, NULL, MIN(first_game_date), MAX(last_game_date), SUM(games) "
        "FROM player_team_seasons WHERE league = ? GROUP BY athlete_id "
        "ON CONFLICT (league, athlete_id) DO UPDATE SET "
        "first_game_date = excluded.first_game_date, last_game_date = excluded.last_game_date, "
        "games = excluded.games", (league, league))
    conn.execute(
        "UPDATE players SET team_id = (SELECT pts.team_id FROM player_team_seasons pts "
        "WHERE pts.league = players.league AND pts.athlete_id = players.athlete_id "
        "ORDER BY pts.last_game_date DESC, pts.season DESC LIMIT 1) WHERE league = ?",
        (league,))
    conn.execute("DELETE FROM players WHERE league = ? AND games IS NULL", (league,))
    conn.commit()


def _upsert_names(conn, league, rows):
    """Name/position: newest game wins; a missing value never overwrites."""
    conn.executemany(
        "INSERT INTO players (league, athlete_id, name, short_name, position, last_game_date) "
        "VALUES (?,?,?,?,?,?) ON CONFLICT (league, athlete_id) DO UPDATE SET "
        "name = CASE WHEN excluded.last_game_date >= COALESCE(players.last_game_date, '') "
        "  THEN COALESCE(excluded.name, players.name) ELSE COALESCE(players.name, excluded.name) END, "
        "short_name = CASE WHEN excluded.last_game_date >= COALESCE(players.last_game_date, '') "
        "  THEN COALESCE(excluded.short_name, players.short_name) ELSE COALESCE(players.short_name, excluded.short_name) END, "
        "position = CASE WHEN excluded.last_game_date >= COALESCE(players.last_game_date, '') "
        "  THEN COALESCE(excluded.position, players.position) ELSE COALESCE(players.position, excluded.position) END, "
        "last_game_date = MAX(COALESCE(players.last_game_date, ''), excluded.last_game_date)",
        [(league, a, *v) for a, v in _player_rows(rows).items()])


def pending(conn, league, full=False):
    """Completed, playable, non-exhibition games with a summary whose player
    rows are missing or stale. Oldest first."""
    ex = db.not_exhibition_sql(db.exhibition_team_ids(conn, league), "g")
    redo = "" if full else (
        "AND (l.espn_event_id IS NULL OR l.status != 'ok' OR l.version < ? "
        "OR l.summary_fetched_at != s.fetched_at)")
    args = (league,) if full else (league, PARSER_VERSION)
    return [r[0] for r in conn.execute(
        "SELECT g.espn_event_id FROM games g JOIN game_summaries s USING (espn_event_id) "
        "LEFT JOIN player_extract_log l USING (espn_event_id) "
        f"WHERE g.league = ? AND g.completed = 1 AND g.season_type IN (2, 3) "
        f"AND {db.playable_sql('g')} AND {ex} {redo} ORDER BY g.game_date, g.espn_event_id",
        args)]


def skipped_counts(conn, league):
    """Why stored summaries were left out (logged each run, never silent)."""
    ex = db.exhibition_team_ids(conn, league)
    c = Counter()
    for st, comp, stype, h, a in conn.execute(
            "SELECT g.status, g.completed, g.season_type, g.home_team_id, g.away_team_id "
            "FROM games g JOIN game_summaries s USING (espn_event_id) WHERE g.league = ?",
            (league,)):
        if h in ex or a in ex:
            c["exhibition"] += 1
        elif (st or "") in db.HIDDEN_STATUSES:
            c[st.lower().replace("status_", "")] += 1
        elif not comp:
            c["not_final"] += 1
        elif stype not in (2, 3):
            c["season_type"] += 1
    return dict(c)


def extract(conn, leagues=LEAGUES, limit=None, full=False, log=print, batch=100,
            pause=0.0):
    """Extract pending games per league. Returns {league: stats}."""
    res = {}
    for lg in leagues:
        t0 = time.monotonic()
        ids = pending(conn, lg, full)
        if limit:
            ids = ids[:limit]
        st = Counter()
        cols = {t: [c for c in _columns(conn, t)] for t in TABLES[lg]}
        espn_to_team = {int(k): v for k, v in db.team_id_map(conn, lg).items()}
        notes = Counter()
        conn.commit()
        conn.execute("BEGIN IMMEDIATE")
        for n, eid in enumerate(ids, 1):
            g = dict(conn.execute("SELECT * FROM games WHERE espn_event_id = ?", (eid,)).fetchone())
            fetched, gz = conn.execute("SELECT fetched_at, json_gz FROM game_summaries "
                                       "WHERE espn_event_id = ?", (eid,)).fetchone()
            status, nrows, note = "ok", 0, None
            try:
                rows, no_id = parse_game(json.loads(gzip.decompress(gz)), g, espn_to_team)
                if no_id:
                    st["lines_without_athlete_id"] += no_id
                if not rows:
                    status = "no_box"
                nrows = store_game(conn, lg, eid, rows, cols)
                if rows:
                    _upsert_names(conn, lg, rows)
                    if lg == "mlb":
                        st["mlb_first_pitcher_not_flagged_starter"] += sum(
                            bool(r.get("_flag")) for r in rows.get("player_game_mlb_pitching", []))
            except (BoxError, OSError, ValueError, KeyError, TypeError) as e:
                status, note = "error", f"{type(e).__name__}: {e}"[:300]
                notes[note.split(":")[0] + ": " + str(e)[:60]] += 1
                for t in TABLES[lg]:
                    conn.execute(f"DELETE FROM {t} WHERE espn_event_id = ?", (eid,))
            conn.execute(
                "INSERT OR REPLACE INTO player_extract_log (espn_event_id, league, "
                "summary_fetched_at, extracted_at, version, status, rows, note) "
                "VALUES (?,?,?,?,?,?,?,?)", (eid, lg, fetched, _now(), PARSER_VERSION,
                                             status, nrows, note))
            st[status] += 1
            st["rows"] += nrows
            if n % batch == 0:
                conn.commit()
                log(f"players {lg}: {n}/{len(ids)} games ({dict(st)})")
                if pause:
                    time.sleep(pause)
                conn.execute("BEGIN IMMEDIATE")
        conn.commit()
        if ids:
            refresh_players(conn, lg)
        st = dict(st, games=len(ids), seconds=round(time.monotonic() - t0, 1),
                  skipped=skipped_counts(conn, lg))
        if notes:
            st["errors_by_kind"] = dict(notes.most_common(10))
        log(f"players {lg}: {st}")
        res[lg] = st
    return res


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--db", default="data/bayesratings.db")
    ap.add_argument("--leagues", default=",".join(LEAGUES))
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--full", action="store_true")
    ap.add_argument("--pause", type=float, default=0.2)
    a = ap.parse_args(argv)
    leagues = [x for x in a.leagues.split(",") if x]
    conn = db.connect(a.db)
    conn.isolation_level = None
    db.init_db(conn)
    extract(conn, leagues, a.limit, a.full, pause=a.pause,
            log=lambda m: print(m, flush=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
