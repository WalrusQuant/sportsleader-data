"""Team Lab raw counts, one row per team per game, from stored summaries.

Copied from bayesratings/labstats.py. The COUNTS map is the export contract.
The extract() below is the same function the nightly job calls.
"""
import gzip
import json
import re
import time

from . import db

VERSION = 4
LEAGUES = ("mlb", "nba", "nfl", "nhl")
_CUP_NAME = re.compile(r"nba\s*cup|in[-\s]?season\s+tournament", re.I)
_CUP_CHAMP = re.compile(r"championship", re.I)
COUNTS = {
    "nfl": ("yd", "plays", "net_pass", "cmp", "att", "sacks", "sack_yds",
            "rush_yd", "rush_att", "fd_pass", "fd_rush", "fd_pen",
            "d3m", "d3a", "d4m", "d4a", "rz_td", "rz_att",
            "pen", "pen_yds", "to", "top", "drives", "score_dr",
            "def_sk", "def_tfl", "def_pd", "def_qbh"),
    "nba": ("fgm", "fga", "tpm", "tpa", "ftm", "fta",
            "oreb", "dreb", "ast", "stl", "blk", "tov", "pf",
            "fbp", "pip", "pot", "poss", "minutes"),
    "nhl": ("sog", "hits", "blk", "tk", "gv", "ppg", "ppo", "shg",
            "pim", "pen", "fo", "sa", "sv"),
    "mlb": ("ab", "h", "dbl", "tpl", "hr", "bb", "hbp", "k", "sb", "cs",
            "sf", "tb", "pa", "lob",
            "p_outs", "p_h", "p_er", "p_bb", "p_k", "p_hr", "p_hbp",
            "qs", "sv", "bs", "bp_outs", "bp_er",
            "po", "a", "e", "dp"),
}
_CLOCK = {"END OF HALF", "END OF GAME", "END OF REGULATION"}


def _clean(v):
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, float):
        v = round(v, 2)
        if v == int(v):
            return int(v)
    return v


def _num(v):
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return _clean(v)
    s = str(v).strip().replace(",", "")
    if s in ("", "-", "--", "---") or s.lower() == "nan":
        return None
    try:
        x = float(s)
    except ValueError:
        return None
    return _clean(x)


def _pair(v, sep="-"):
    s = "" if v is None else str(v).strip()
    if s.count(sep) != 1:
        return None
    a, b = s.split(sep)
    if not a or not b or a.startswith("-") or b.startswith("-"):
        return None
    ai, bi = _num(a), _num(b)
    if ai is None or bi is None:
        return None
    return ai, bi


def _clock(v):
    s = "" if v is None else str(v).strip()
    if ":" not in s:
        return None
    m, _, sec = s.partition(":")
    mi, si = _num(m), _num(sec)
    if mi is None or si is None:
        return None
    return int(mi) * 60 + int(si)


def _outs(ip):
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


def _flat_pairs(keys, stats):
    if len(keys) != len(stats):
        return {}
    raw = {k: "" if v is None else str(v) for k, v in zip(keys, stats)}
    flat = dict(raw)
    for k, v in raw.items():
        for sep in ("-", "/"):
            if k.count(sep) == 1 and "." not in k and not v.startswith("-"):
                parts = v.split(sep)
                if len(parts) == 2:
                    a, b = k.split(sep)
                    flat.setdefault(a, parts[0])
                    flat.setdefault(b, parts[1])
    return flat


def _box_teams(summary):
    out = {}
    for t in (summary.get("boxscore") or {}).get("teams") or []:
        eid = str((t.get("team") or {}).get("id") or "")
        if eid:
            out[eid] = t
    return out


def _player_teams(summary):
    out = {}
    for t in (summary.get("boxscore") or {}).get("players") or []:
        eid = str((t.get("team") or {}).get("id") or "")
        if eid:
            out[eid] = t
    return out


def _team_flat(team):
    out = {}
    for st in team.get("statistics") or []:
        if st.get("stats"):
            continue
        name = st.get("name")
        if not name:
            continue
        dv = st.get("displayValue")
        out[name] = dv if dv not in (None, "") else st.get("value")
    return out


def _group_num(team, group, name):
    for st in team.get("statistics") or []:
        if st.get("name") != group:
            continue
        for x in st.get("stats") or []:
            if x.get("name") == name:
                return _stat_num(x)
    return None


def _stat_num(stat):
    if not isinstance(stat, dict):
        return None
    v = stat.get("value")
    if isinstance(v, bool):
        v = None
    if isinstance(v, (int, float)):
        return _clean(v)
    return _num(stat.get("displayValue"))


_TEAM_LOB_NAMES = ("teamLeftOnBase", "teamLOB", "LOB")


def _first_stat(stats, names):
    found = {}
    for st in stats or []:
        name = st.get("name") if isinstance(st, dict) else None
        if name in names and name not in found:
            found[name] = st
    for name in names:
        if name not in found:
            continue
        n = _stat_num(found[name])
        if n is not None:
            return n
    return None


def _team_lob(team):
    for block in team.get("details") or []:
        if not isinstance(block, dict):
            continue
        names = _TEAM_LOB_NAMES if block.get("name") in ("battingDetails", "batting") else (
            "teamLeftOnBase", "teamLOB")
        n = _first_stat(block.get("stats"), names)
        if n is not None:
            return n
    for st in team.get("statistics") or []:
        if st.get("name") != "batting":
            continue
        n = _first_stat(st.get("stats"), _TEAM_LOB_NAMES)
        if n is not None:
            return n
    return _derive_team_lob(team)


def _derive_team_lob(team):
    hits, runs = _group_num(team, "batting", "hits"), _group_num(team, "batting", "runs")
    if hits is None or runs is None:
        return None

    def z(name):
        v = _group_num(team, "batting", name)
        return 0 if v is None else v

    lob = hits + z("walks") + z("hitByPitch") - runs - z("caughtStealing") - z("GIDPs")
    if lob < 0:
        lob = 0
    return _clean(lob)


def _pack(league, d):
    if d is None:
        return None
    return [_clean(d.get(k)) for k in COUNTS[league]]


def nba_nonstanding(game_note):
    text = game_note or ""
    return bool(_CUP_NAME.search(text) and _CUP_CHAMP.search(text))


def _game_note(summary):
    if not summary:
        return ""
    note = (summary.get("header") or {}).get("gameNote")
    return note.strip() if isinstance(note, str) else ""


def _nba_minutes(summary):
    comp = (((summary.get("header") or {}).get("competitions") or [{}])[0]
            .get("competitors") or [])
    periods = max((len(c.get("linescores") or []) for c in comp), default=0)
    if periods >= 4:
        return 48 + 5 * (periods - 4)
    return None


def _drives(summary, eid, flat):
    drives = (summary.get("drives") or {}).get("previous") or []
    mine = [d for d in drives
            if str((d.get("team") or {}).get("id")) == str(eid)
            and (d.get("result") or "").upper() not in _CLOCK]
    if mine:
        scoring = sum((d.get("result") or "").upper() in ("TD", "FG") for d in mine)
        return len(mine), scoring
    total = _num(flat.get("totalDrives"))
    return (int(total) if total is not None else None), None


def _sum_category(players, eid, category, names):
    team = players.get(str(eid))
    if not team:
        return {n: None for n in names}
    for g in team.get("statistics") or []:
        if g.get("name") != category:
            continue
        keys = g.get("keys") or []
        acc = {n: 0 for n in names}
        seen = {n: False for n in names}
        for a in g.get("athletes") or []:
            flat = _flat_pairs(keys, a.get("stats") or [])
            if not flat and (a.get("stats") or []):
                continue
            for n in names:
                val = _num(flat.get(n))
                if val is not None:
                    acc[n] += val
                    seen[n] = True
        return {n: (_clean(acc[n]) if seen[n] else 0) for n in names}
    return {n: None for n in names}


def _goalie_sv(players, eid):
    team = players.get(str(eid))
    if not team:
        return None, None
    sa = sv = 0
    seen = False
    found = False
    for g in team.get("statistics") or []:
        if g.get("name") != "goalies":
            continue
        found = True
        keys = g.get("keys") or []
        for a in g.get("athletes") or []:
            flat = _flat_pairs(keys, a.get("stats") or [])
            s1, s2 = _num(flat.get("shotsAgainst")), _num(flat.get("saves"))
            if s1 is not None:
                sa += s1
                seen = True
            if s2 is not None:
                sv += s2
                seen = True
    if not found:
        return None, None
    if not seen:
        return 0, 0
    return _clean(sa), _clean(sv)


def _bullpen(players, eid):
    team = players.get(str(eid))
    if not team:
        return None, None
    for g in team.get("statistics") or []:
        keys = g.get("keys") or []
        if "fullInnings.partInnings" not in keys:
            continue
        outs = er = 0
        for i, a in enumerate(g.get("athletes") or [], 1):
            if i == 1:
                continue
            flat = _flat_pairs(keys, a.get("stats") or [])
            o = _outs(flat.get("fullInnings.partInnings"))
            e = _num(flat.get("earnedRuns"))
            if o is not None:
                outs += o
            if e is not None:
                er += e
        return outs, er
    return None, None


def _nfl(flat, summary, eid, players):
    cmp_att = _pair(flat.get("completionAttempts"), "/")
    sacks = _pair(flat.get("sacksYardsLost"))
    d3 = _pair(flat.get("thirdDownEff"))
    d4 = _pair(flat.get("fourthDownEff"))
    rz = _pair(flat.get("redZoneAttempts"))
    pen = _pair(flat.get("totalPenaltiesYards"))
    drives, score_dr = _drives(summary, eid, flat)
    defs = _sum_category(players, eid, "defensive",
                         ("sacks", "tacklesForLoss", "passesDefended", "QBHits"))
    return {
        "yd": _num(flat.get("totalYards")),
        "plays": _num(flat.get("totalOffensivePlays")),
        "net_pass": _num(flat.get("netPassingYards")),
        "cmp": cmp_att[0] if cmp_att else None,
        "att": cmp_att[1] if cmp_att else None,
        "sacks": sacks[0] if sacks else None,
        "sack_yds": sacks[1] if sacks else None,
        "rush_yd": _num(flat.get("rushingYards")),
        "rush_att": _num(flat.get("rushingAttempts")),
        "fd_pass": _num(flat.get("firstDownsPassing")),
        "fd_rush": _num(flat.get("firstDownsRushing")),
        "fd_pen": _num(flat.get("firstDownsPenalty")),
        "d3m": d3[0] if d3 else None, "d3a": d3[1] if d3 else None,
        "d4m": d4[0] if d4 else None, "d4a": d4[1] if d4 else None,
        "rz_td": rz[0] if rz else None, "rz_att": rz[1] if rz else None,
        "pen": pen[0] if pen else None, "pen_yds": pen[1] if pen else None,
        "to": _num(flat.get("turnovers")),
        "top": _clock(flat.get("possessionTime")),
        "drives": drives, "score_dr": score_dr,
        "def_sk": defs["sacks"], "def_tfl": defs["tacklesForLoss"],
        "def_pd": defs["passesDefended"], "def_qbh": defs["QBHits"],
    }


def _nba(flat, summary):
    fg = _pair(flat.get("fieldGoalsMade-fieldGoalsAttempted"))
    tp = _pair(flat.get("threePointFieldGoalsMade-threePointFieldGoalsAttempted"))
    ft = _pair(flat.get("freeThrowsMade-freeThrowsAttempted"))
    oreb = _num(flat.get("offensiveRebounds"))
    tov = _num(flat.get("totalTurnovers"))
    if tov is None:
        tov = _num(flat.get("turnovers"))
    return {
        "fgm": fg[0] if fg else None, "fga": fg[1] if fg else None,
        "tpm": tp[0] if tp else None, "tpa": tp[1] if tp else None,
        "ftm": ft[0] if ft else None, "fta": ft[1] if ft else None,
        "oreb": oreb, "dreb": _num(flat.get("defensiveRebounds")),
        "ast": _num(flat.get("assists")), "stl": _num(flat.get("steals")),
        "blk": _num(flat.get("blocks")), "tov": tov,
        "pf": _num(flat.get("fouls")),
        "fbp": _num(flat.get("fastBreakPoints")),
        "pip": _num(flat.get("pointsInPaint")),
        "pot": _num(flat.get("turnoverPoints")),
        "poss": None, "minutes": _nba_minutes(summary),
    }


def _bbref_poss(a, b):
    def side(tm, opp):
        fgm, fga = tm.get("fgm"), tm.get("fga")
        fta, oreb, tov = tm.get("fta"), tm.get("oreb"), tm.get("tov")
        opp_dreb = opp.get("dreb")
        if None in (fgm, fga, fta, oreb, tov, opp_dreb):
            return None
        denom = oreb + opp_dreb
        share = (oreb / denom) if denom else 0.0
        return fga + 0.4 * fta - 1.07 * share * (fga - fgm) + tov

    sa, sb = side(a, b), side(b, a)
    if sa is None or sb is None:
        return None
    return _clean(0.5 * (sa + sb))


def _nhl(flat, players, eid):
    sa, sv = _goalie_sv(players, eid)
    return {
        "sog": _num(flat.get("shotsTotal")),
        "hits": _num(flat.get("hits")),
        "blk": _num(flat.get("blockedShots")),
        "tk": _num(flat.get("takeaways")),
        "gv": _num(flat.get("giveaways")),
        "ppg": _num(flat.get("powerPlayGoals")),
        "ppo": _num(flat.get("powerPlayOpportunities")),
        "shg": _num(flat.get("shortHandedGoals")),
        "pim": _num(flat.get("penaltyMinutes")),
        "pen": _num(flat.get("penalties")),
        "fo": _num(flat.get("faceoffsWon")),
        "sa": sa, "sv": sv,
    }


def _mlb(team, players, eid):
    g = lambda group, name: _group_num(team, group, name)  # noqa: E731
    outs = g("pitching", "thirdInnings")
    if outs is None:
        full, part = g("pitching", "fullInnings"), g("pitching", "partInnings")
        if full is not None and part is not None:
            outs = int(full) * 3 + int(part)
    bp_outs, bp_er = _bullpen(players, eid)
    return {
        "ab": g("batting", "atBats"), "h": g("batting", "hits"),
        "dbl": g("batting", "doubles"), "tpl": g("batting", "triples"),
        "hr": g("batting", "homeRuns"), "bb": g("batting", "walks"),
        "hbp": g("batting", "hitByPitch"), "k": g("batting", "strikeouts"),
        "sb": g("batting", "stolenBases"), "cs": g("batting", "caughtStealing"),
        "sf": g("batting", "sacFlies"), "tb": g("batting", "totalBases"),
        "pa": g("batting", "plateAppearances"),
        "lob": _team_lob(team),
        "p_outs": outs, "p_h": g("pitching", "hits"),
        "p_er": g("pitching", "earnedRuns"), "p_bb": g("pitching", "walks"),
        "p_k": g("pitching", "strikeouts"), "p_hr": g("pitching", "homeRuns"),
        "p_hbp": g("pitching", "battersHit"),
        "qs": g("pitching", "qualityStarts"),
        "sv": g("pitching", "saves"), "bs": g("pitching", "blownSaves"),
        "bp_outs": bp_outs, "bp_er": bp_er,
        "po": g("fielding", "putouts"), "a": g("fielding", "assists"),
        "e": g("fielding", "errors"), "dp": g("fielding", "doublePlays"),
    }


def parse(summary, league, espn_ids):
    boxes = _box_teams(summary)
    players = _player_teams(summary)
    ids = []
    raw = {}
    for eid in espn_ids:
        eid = str(eid) if eid is not None else ""
        ids.append(eid)
        team = boxes.get(eid)
        if not team:
            raw[eid] = None
            continue
        try:
            if league == "nfl":
                d = _nfl(_team_flat(team), summary, eid, players)
            elif league == "nba":
                d = _nba(_team_flat(team), summary)
            elif league == "nhl":
                d = _nhl(_team_flat(team), players, eid)
            elif league == "mlb":
                d = _mlb(team, players, eid)
            else:
                d = None
        except (TypeError, ValueError, KeyError, IndexError, AttributeError):
            d = None
        raw[eid] = d
    if league == "nba":
        have = [eid for eid in ids if raw.get(eid)]
        poss = _bbref_poss(raw[have[0]], raw[have[1]]) if len(have) == 2 else None
        for eid in have:
            raw[eid]["poss"] = poss
    return {eid: _pack(league, raw[eid]) for eid in ids}


def _pending(conn, league):
    ex = db.not_exhibition_sql(db.exhibition_team_ids(conn, league), "g")
    return conn.execute(
        "SELECT g.espn_event_id, g.home_team_id, g.away_team_id, g.season, "
        "g.season_type, g.game_date, g.home_score, g.away_score, s.fetched_at "
        "FROM games g JOIN game_summaries s USING (espn_event_id) "
        f"WHERE g.league = ? AND g.completed = 1 AND g.season_type IN (2, 3) "
        f"AND g.home_score IS NOT NULL AND g.away_score IS NOT NULL "
        f"AND {db.playable_sql('g')} AND {ex} AND NOT EXISTS ("
        "SELECT 1 FROM team_game_stats y WHERE y.espn_event_id = g.espn_event_id "
        "AND y.version = ? AND y.summary_fetched_at = s.fetched_at "
        "GROUP BY y.espn_event_id HAVING COUNT(*) = 2) "
        "ORDER BY g.game_date, g.espn_event_id",
        (league, VERSION)).fetchall()


def _dump(counts):
    return json.dumps(counts, separators=(",", ":"))


def extract(conn, leagues=LEAGUES, log=print, batch=200):
    """Fill team_game_stats for finished games whose rows are missing or stale."""
    res = {}
    espn = {r[0]: str(r[1]) for r in conn.execute("SELECT team_id, espn_id FROM teams")}
    for lg in leagues:
        if lg not in LEAGUES:
            continue
        t0 = time.monotonic()
        todo = _pending(conn, lg)
        ok = bad = 0
        conn.commit()
        conn.execute("BEGIN IMMEDIATE")
        for n, (eid, h, a, season, stype, gdate, hs, aws, fetched) in enumerate(todo, 1):
            gz = conn.execute(
                "SELECT json_gz FROM game_summaries WHERE espn_event_id = ?",
                (eid,)).fetchone()[0]
            summary = None
            try:
                summary = json.loads(gzip.decompress(gz))
                vals = parse(summary, lg, [espn.get(h), espn.get(a)])
            except (OSError, ValueError, KeyError, TypeError, AttributeError, IndexError):
                vals = {}
            if lg == "nba" and nba_nonstanding(_game_note(summary)):
                stype = 0
            sides = ((h, a, hs, aws), (a, h, aws, hs))
            for tid, opp, pf, pa in sides:
                mine = vals.get(espn.get(tid))
                theirs = vals.get(espn.get(opp))
                if mine:
                    ok += 1
                else:
                    bad += 1
                conn.execute(
                    "INSERT OR REPLACE INTO team_game_stats ("
                    "espn_event_id, team_id, opp_team_id, league, season, season_type, "
                    "game_date, is_home, points_for, points_against, summary_fetched_at, "
                    "version, counts_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (eid, tid, opp, lg, season, stype, gdate, int(tid == h), pf, pa,
                     fetched, VERSION, _dump([mine, theirs])))
            if n % batch == 0:
                conn.commit()
                conn.execute("BEGIN IMMEDIATE")
        conn.commit()
        res[lg] = {"games": len(todo), "ok": ok, "unreadable": bad,
                   "seconds": round(time.monotonic() - t0, 1)}
        log(f"labstats {lg}: {res[lg]}")
    return res
