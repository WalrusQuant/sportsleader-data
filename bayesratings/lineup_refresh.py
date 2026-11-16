"""Refresh NBA.com starting lineups and official NHL goalies and lines.

The nightly job used to keep these rows and not re-fetch them. Two things
made a full copy of the old passes a poor fit for a short run:

- NBA.com publishes one JSON file per day
  (stats.nba.com ``00_daily_lineups_YYYYMMDD.json``). The file is often
  missing until the slate is posted, and a 404 is normal. Confirmed fives
  replace the morning "Expected" fives, so only the recent window matters.
- NHL pre-game goalies are already on the ESPN scoreboard (``game_probables``,
  kind SG). The official NHL API has no projected lines and no pre-game
  starter. Once a game is final, the box score flags the starter and the
  shift chart has every even-strength shift. Lines and defense pairs are the
  forward trios and defense pairs that shared 5-on-5 time.

This pass only covers the last ``LOOKBACK_DAYS`` through tomorrow. NHL
fetches are limited to completed games in that window that are not already
stored with source ``nhl_api``, so a nightly run reads the new slate and
does not walk the season. A page that is missing or unparsable is logged
and skipped. Nothing here raises: the rest of the nightly publish still runs.
"""

import gzip
import hashlib
import json
import re
import time
import unicodedata
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

from . import db

LOOKBACK_DAYS = 8
LOOKAHEAD_DAYS = 1
# Stop fetching rather than let a slow source hold the publish.
BUDGET_S = 75
# 5v5 units shorter than this are line-change noise, not a deployed line.
MIN_UNIT_SECONDS = 120
NBA_URL = "https://stats.nba.com/js/data/leaders/00_daily_lineups_{d}.json"
DEPTH_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/{espn_id}/depthcharts"
NHL_SCHEDULE = "https://api-web.nhle.com/v1/schedule/{day}"
NHL_BOX = "https://api-web.nhle.com/v1/gamecenter/{gid}/boxscore"
NHL_SHIFTS = "https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId={gid}"
NHL_SOURCE = "nhl_api"
NHL_FINAL = {"OFF", "FINAL"}
UA = "bayesratings/0.1 (bayesratings.com; low-frequency pre-game data logging)"
SLOTS = ("PG", "SG", "SF", "PF", "C")
NBA_ABBR = {
    "NOP": "NO", "UTA": "UTAH", "GSW": "GS", "SAS": "SA", "NYK": "NY",
    "WAS": "WSH", "BRK": "BKN", "PHO": "PHX",
}
NHL_ABBR = {"LAK": "LA", "NJD": "NJ", "SJS": "SJ", "TBL": "TB", "UTA": "UTAH"}
_SUFFIX = re.compile(r"\b(jr|sr|ii|iii|iv|v)\b")
_POS = {"C": "c", "L": "lw", "R": "rw", "D": "d", "G": "g"}
_KIND = {"forwards": "F", "defense": "D", "goalies": "G"}
_POS_RANK = {"lw": 0, "c": 1, "rw": 2, "d": 3, "g": 4}


class Missing(Exception):
    """403/404, or a page that is not posted yet. Not a failed publish."""


def log_default(msg):
    print(msg, flush=True)


def challenged(body) -> bool:
    head = body[:6000].lower() if isinstance(body, (bytes, bytearray)) else str(body)[:6000].lower().encode()
    return (b"just a moment" in head or b"cdn-cgi/challenge" in head
            or b"cf-mitigated" in head)


def http_get(url, accept="*/*", headers=None, timeout=20, retries=2) -> bytes:
    """GET bytes. 403 and 404 raise Missing. 429/5xx retry briefly."""
    h = {"User-Agent": UA, "Accept": accept, "Accept-Encoding": "gzip"}
    if headers:
        h.update(headers)
    req = urllib.request.Request(url, headers=h)
    delay = 1.0
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                if resp.headers.get("Content-Encoding") == "gzip":
                    raw = gzip.decompress(raw)
                return raw
        except urllib.error.HTTPError as e:
            if e.code in (401, 403, 404):
                raise Missing(f"{e.code} {url}") from None
            if e.code in (429, 500, 502, 503, 504) and attempt < retries:
                time.sleep(delay)
                delay *= 2
                continue
            raise
        except (urllib.error.URLError, TimeoutError):
            if attempt < retries:
                time.sleep(delay)
                delay *= 2
                continue
            raise
    raise RuntimeError(f"unreachable retry loop for {url}")


class Client:
    """JSON fetch. ``gap`` and ``sleep`` are accepted so older callers still load."""

    def __init__(self, get=None, log=log_default, gap=None, sleep=None):
        self.get = get or http_get
        self.log = log
        self.n = 0

    def json(self, url) -> bytes:
        self.n += 1
        body = self.get(url, "application/json", None)
        if challenged(body):
            raise Missing(f"challenge {url}")
        return body


def norm_name(s) -> str:
    """'Moore Jr., Wendell' and 'Wendell Moore Jr.' -> 'wendell moore'.
    Single initials collapse, so 'J. J. Peterka' matches 'JJ Peterka'."""
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower()
    if "," in s:
        last, _, first = s.partition(",")
        s = f"{first} {last}"
    s = re.sub(r"[.'`\-]", " ", s)
    s = _SUFFIX.sub(" ", s)
    collapsed, buf = [], ""
    for part in s.split():
        if len(part) == 1:
            buf += part
        else:
            if buf:
                collapsed.append(buf)
                buf = ""
            collapsed.append(part)
    if buf:
        collapsed.append(buf)
    return " ".join(collapsed)


def norm_team(s) -> str:
    text = unicodedata.normalize("NFKD", str(s or "")).encode("ascii", "ignore").decode().lower()
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]", "", text)).strip()


def _eq(a, b) -> bool:
    if a is None or a == "":
        return b is None or b == ""
    if b is None or b == "":
        return False
    if isinstance(a, bool) or isinstance(b, bool):
        return bool(a) == bool(b)
    try:
        if isinstance(a, (int, float)) or isinstance(b, (int, float)):
            return int(a) == int(b)
    except (TypeError, ValueError):
        pass
    return str(a).strip() == str(b).strip()


def _hash(fields, row) -> str:
    payload = json.dumps([row.get(f) for f in fields], default=str)
    return hashlib.sha1(payload.encode()).hexdigest()[:16]


class Teams:
    def __init__(self, conn, league, abbr_map):
        self.by_name, self.by_abbr, self.espn = {}, {}, {}
        for tid, espn_id, name, abbr in conn.execute(
                "SELECT team_id, espn_id, name, abbreviation FROM teams WHERE league = ?",
                (league,)):
            self.espn[tid] = espn_id
            if name:
                self.by_name[norm_team(name)] = tid
            if abbr:
                self.by_abbr[str(abbr).upper()] = tid
        self.abbr_map = abbr_map

    def by(self, name=None, abbr=None):
        if name and norm_team(name) in self.by_name:
            return self.by_name[norm_team(name)]
        if abbr:
            key = self.abbr_map.get(abbr.upper(), abbr.upper())
            if key in self.by_abbr:
                return self.by_abbr[key]
        return None


class Names:
    """Roster name, then a league-wide unique name, to an ESPN athlete id."""

    def __init__(self, conn, league):
        self.team, self.wide = {}, {}
        for tid, aid, name in conn.execute(
                "SELECT team_id, athlete_id, name FROM roster_states "
                "WHERE league = ? AND valid_to IS NULL AND name IS NOT NULL", (league,)):
            self.team[(tid, norm_name(name))] = int(aid)
            self.wide.setdefault(norm_name(name), set()).add(int(aid))
        for aid, name in conn.execute(
                "SELECT athlete_id, name FROM players WHERE league = ? AND name IS NOT NULL",
                (league,)):
            self.wide.setdefault(norm_name(name), set()).add(int(aid))

    def match(self, name, team_id):
        key = norm_name(name)
        if not key:
            return None
        if team_id is not None and (team_id, key) in self.team:
            return self.team[(team_id, key)]
        ids = self.wide.get(key) or set()
        if len(ids) == 1:
            return next(iter(ids))
        return None

    def match_initial(self, name, team_id):
        """'T. Jarry' -> the one current roster name on this team with that
        last name and first initial. Ambiguous and unknown names return None."""
        key = norm_name(name)
        parts = key.split()
        if team_id is None or len(parts) < 2 or len(parts[0]) != 1:
            return None
        initial, last = parts[0], parts[-1]
        hits = []
        for (tid, roster_name), aid in self.team.items():
            if tid != team_id:
                continue
            bits = roster_name.split()
            if len(bits) >= 2 and bits[-1] == last and bits[0].startswith(initial):
                hits.append(aid)
        if len(hits) == 1:
            return hits[0]
        return None


def _stamp(now) -> str:
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    return now.astimezone(timezone.utc).isoformat(timespec="seconds")


def _window(today):
    start = today - timedelta(days=LOOKBACK_DAYS)
    end = today + timedelta(days=LOOKAHEAD_DAYS)
    return start, end


def _dates(conn, league, start, end):
    rows = conn.execute(
        "SELECT DISTINCT game_date FROM games WHERE league = ? AND season_type IN (2, 3) "
        f"AND {db.playable_sql()} AND game_date BETWEEN ? AND ? ORDER BY game_date",
        (league, start.isoformat(), end.isoformat()))
    return [r[0] for r in rows]


def _starter(player, abbr) -> dict:
    name = player.get("playerName") or " ".join(
        x for x in (player.get("firstName"), player.get("lastName")) if x)
    return {
        "team_abbr": abbr,
        "slot": (player.get("position") or "").upper(),
        "name": name,
        "status": player.get("lineupStatus") or None,
        "nba_person_id": player.get("personId"),
    }


def parse_nba(payload) -> list:
    """One dict per game: home/away abbreviation and that side's starters."""
    out = []
    for game in (payload or {}).get("games") or []:
        sides = {}
        abbrs = {}
        for side, key in (("home", "homeTeam"), ("away", "awayTeam")):
            team = game.get(key) or {}
            abbrs[side] = team.get("teamAbbreviation")
            starters = []
            for player in team.get("players") or []:
                row = _starter(player, abbrs[side])
                if row["slot"] in SLOTS:
                    starters.append(row)
            sides[side] = starters
        out.append({"home_abbr": abbrs.get("home"), "away_abbr": abbrs.get("away"), **sides})
    return out


def depth_starters(payload) -> list:
    charts = (payload or {}).get("depthchart") or []
    positions = (charts[0].get("positions") if charts else None) or {}
    used, out = set(), []
    for slot in SLOTS:
        for athlete in (positions.get(slot.lower()) or {}).get("athletes") or []:
            aid = int(athlete["id"]) if athlete.get("id") else None
            if aid and aid not in used:
                used.add(aid)
                out.append((slot, aid, athlete.get("displayName")))
                break
    return out


def _nba_event(conn, home, away, day):
    """ESPN event for this home/away pair on ``day`` (or the adjacent date)."""
    best = None
    for eid, gdate in conn.execute(
            "SELECT espn_event_id, game_date FROM games WHERE league = 'nba' "
            "AND ((home_team_id = ? AND away_team_id = ?) OR (home_team_id = ? AND away_team_id = ?))",
            (home, away, away, home)):
        try:
            dist = abs((datetime.fromisoformat(str(gdate)[:10]).date() - day).days)
        except ValueError:
            continue
        swapped = False
        # Prefer the true home side when both orientations are in range.
        home_ok = conn.execute(
            "SELECT 1 FROM games WHERE espn_event_id = ? AND home_team_id = ?",
            (eid, home)).fetchone() is not None
        if dist <= 1 and (best is None or dist < best[0] or (dist == best[0] and home_ok and not best[2])):
            swapped = not home_ok
            best = (dist, eid, home_ok, swapped)
    if best is None:
        return None
    return best[1]


def _clock(text):
    if not text or not isinstance(text, str) or ":" not in text:
        return None
    parts = text.split(":")
    try:
        nums = [int(p) for p in parts]
    except ValueError:
        return None
    if len(nums) == 2:
        return nums[0] * 60 + nums[1]
    if len(nums) == 3:
        return nums[0] * 3600 + nums[1] * 60 + nums[2]
    return None


def _loads(body):
    if isinstance(body, (bytes, bytearray)):
        return json.loads(body.decode("utf-8"))
    return json.loads(body)


def _side_name(team):
    if not team:
        return None, None
    place = ((team.get("placeName") or {}).get("default")) or ""
    common = ((team.get("commonName") or {}).get("default")) or ""
    name = f"{place} {common}".strip() or None
    return team.get("abbrev"), name


def iter_schedule(payload):
    """NHL schedule week -> one dict per game."""
    out = []
    for block in (payload or {}).get("gameWeek") or []:
        day = block.get("date")
        for game in block.get("games") or []:
            away_abbr, away_name = _side_name(game.get("awayTeam"))
            home_abbr, home_name = _side_name(game.get("homeTeam"))
            out.append({
                "nhl_id": game.get("id"),
                "date": day,
                "start": game.get("startTimeUTC"),
                "state": game.get("gameState"),
                "away_abbr": away_abbr,
                "home_abbr": home_abbr,
                "away_name": away_name,
                "home_name": home_name,
            })
    return out


def box_roster(box) -> dict:
    """player id -> kind (F/D/G), position id, abbreviated name, team abbrev."""
    out = {}
    stats = (box or {}).get("playerByGameStats") or {}
    for side in ("awayTeam", "homeTeam"):
        abbr = ((box or {}).get(side) or {}).get("abbrev")
        groups = stats.get(side) or {}
        for key, kind in _KIND.items():
            for player in groups.get(key) or []:
                pid = player.get("playerId")
                if pid is None:
                    continue
                raw = (player.get("position") or "").upper()
                name = (player.get("name") or {}).get("default")
                out[int(pid)] = {
                    "kind": kind,
                    "position_id": _POS.get(raw, raw.lower() or None),
                    "name": name,
                    "team_abbr": abbr,
                    "starter": bool(player.get("starter")),
                }
    return out


def shift_rows(payload):
    if isinstance(payload, dict):
        return list(payload.get("data") or [])
    return list(payload or [])


def shift_names(rows) -> dict:
    """player id -> 'First Last' from the shift chart."""
    names = {}
    for row in rows:
        pid = row.get("playerId")
        if pid is None:
            continue
        full = " ".join(x for x in (row.get("firstName"), row.get("lastName")) if x)
        if full:
            names[int(pid)] = full
    return names


def _on_ice(active):
    """team -> set of player ids whose shift is open."""
    return {
        team: {pid for pid, n in counts.items() if n > 0}
        for team, counts in active.items() if counts
    }


def build_units(shifts, positions, min_seconds=MIN_UNIT_SECONDS) -> dict:
    """Forward lines and defense pairs from shared 5-on-5 time.

    A second counts only when each team has exactly five skaters on the ice
    (goalies are not skaters). A forward line is the three forwards on the
    ice in that second. A defense pair is the two defensemen. Units under
    ``min_seconds`` are dropped. ``positions`` maps a player id to kind
    ``F``, ``D``, or ``G``. Shift-chart first and last names replace the
    box score's abbreviated name when both are present.
    """
    meta = {int(pid): dict(info) for pid, info in (positions or {}).items()}
    intervals = []
    for row in shift_rows(shifts):
        code = row.get("typeCode")
        if code not in (None, 517):
            continue
        period = row.get("period")
        if period not in (1, 2, 3, 4):
            continue
        start, end = _clock(row.get("startTime")), _clock(row.get("endTime"))
        if start is None or end is None or end <= start:
            continue
        pid = row.get("playerId")
        team = row.get("teamAbbrev")
        if pid is None or not team:
            continue
        pid = int(pid)
        full = " ".join(x for x in (row.get("firstName"), row.get("lastName")) if x)
        if full:
            meta.setdefault(pid, {"kind": None, "position_id": None, "name": full})
            meta[pid]["name"] = full
        intervals.append((period, team, start, end, pid))

    trio, pair = Counter(), Counter()
    by_period = defaultdict(list)
    for period, team, start, end, pid in intervals:
        by_period[period].append((start, end, team, pid))
    for rows in by_period.values():
        events = []
        for start, end, team, pid in rows:
            events.append((start, 1, team, pid))
            events.append((end, 0, team, pid))
        events.sort()
        active, prev, i = defaultdict(Counter), None, 0
        while i < len(events):
            stamp = events[i][0]
            if prev is not None and stamp > prev:
                _add_slice(trio, pair, _on_ice(active), meta, stamp - prev)
            while i < len(events) and events[i][0] == stamp:
                _, phase, team, pid = events[i]
                active[team][pid] += 1 if phase else -1
                if active[team][pid] <= 0:
                    del active[team][pid]
                i += 1
            prev = stamp
    return _rank_units(trio, pair, meta, min_seconds)


def _add_slice(trio, pair, on, meta, seconds):
    if seconds <= 0 or len(on) < 2:
        return
    skaters = {}
    for team, pids in on.items():
        skaters[team] = {pid for pid in pids if (meta.get(pid) or {}).get("kind") != "G"}
    counts = list(skaters.values())
    if len(counts) < 2 or any(len(group) != 5 for group in counts):
        return
    for team, group in skaters.items():
        forwards = tuple(sorted(pid for pid in group if (meta.get(pid) or {}).get("kind") == "F"))
        defense = tuple(sorted(pid for pid in group if (meta.get(pid) or {}).get("kind") == "D"))
        if len(forwards) == 3:
            trio[(team, forwards)] += seconds
        if len(defense) == 2:
            pair[(team, defense)] += seconds


def _player_sort_key(meta, pid):
    info = meta.get(pid) or {}
    return (_POS_RANK.get(info.get("position_id"), 9), info.get("name") or "", pid)


def _rank_units(trio, pair, meta, min_seconds) -> dict:
    out = {}

    def emit(counter, prefix):
        grouped = {}
        for (team, pids), seconds in counter.items():
            if seconds < min_seconds:
                continue
            grouped.setdefault(team, []).append((seconds, pids))
        for team, units in grouped.items():
            units.sort(key=lambda item: (-item[0], item[1]))
            bucket = out.setdefault(team, {"lines": [], "pairs": []})
            key = "lines" if prefix == "f" else "pairs"
            for i, (seconds, pids) in enumerate(units, start=1):
                ordered = sorted(pids, key=lambda pid: _player_sort_key(meta, pid))
                bucket[key].append({
                    "group_id": f"{prefix}{i}",
                    "seconds": int(seconds),
                    "players": [{
                        "player_id": pid,
                        "name": (meta.get(pid) or {}).get("name"),
                        "position_id": (meta.get(pid) or {}).get("position_id"),
                    } for pid in ordered],
                })

    emit(trio, "f")
    emit(pair, "d")
    return out


def _athlete(names, team_id, full, abbreviated):
    if full:
        hit = names.match(full, team_id)
        if hit:
            return hit
    if abbreviated:
        hit = names.match(abbreviated, team_id)
        if hit:
            return hit
        return names.match_initial(abbreviated, team_id)
    return None


def _insert(conn, table, row, compare, at):
    row = dict(row)
    row["raw_json"] = "{}"
    row["state_hash"] = _hash(compare, row)
    row["valid_from"] = at
    row["last_seen"] = at
    cols = [c for c in row if c != "id"]
    holders = ",".join("?" * len(cols))
    conn.execute(
        f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({holders})",
        [row[c] for c in cols])


def _close(conn, table, rid, at):
    conn.execute(f"UPDATE {table} SET valid_to = ? WHERE id = ? AND valid_to IS NULL", (at, rid))


def apply_nba_team(conn, eid, team_id, rows, at) -> dict:
    """Change-only replace of one team's five. Identical open rows stay put,
    so last_seen from the published file is not rewritten every night."""
    compare = ("athlete_id", "name", "status", "src")
    open_rows = {}
    for rid, slot, aid, name, status, src in conn.execute(
            "SELECT id, slot, athlete_id, name, status, src FROM nba_lineup_states "
            "WHERE espn_event_id = ? AND team_id = ? AND valid_to IS NULL", (eid, team_id)):
        open_rows.setdefault(slot, []).append(
            {"id": rid, "athlete_id": aid, "name": name, "status": status, "src": src})
    st = {"new": 0, "changed": 0, "closed": 0, "same": 0}
    seen = set()
    for row in rows:
        slot = row["slot"]
        cur = next((r for r in open_rows.get(slot, []) if r["id"] not in seen), None)
        if cur and row.get("athlete_id") is None and cur.get("athlete_id") and _eq(cur["name"], row.get("name")):
            row["athlete_id"] = cur["athlete_id"]
        fresh = {k: row.get(k) for k in compare}
        if cur and all(_eq(cur[k], fresh[k]) for k in compare):
            seen.add(cur["id"])
            st["same"] += 1
            continue
        if cur:
            _close(conn, "nba_lineup_states", cur["id"], at)
            seen.add(cur["id"])
            st["changed"] += 1
        else:
            st["new"] += 1
        _insert(conn, "nba_lineup_states", {
            "league": "nba", "espn_event_id": eid, "team_id": team_id, "slot": slot,
            "athlete_id": row.get("athlete_id"), "name": row.get("name"),
            "status": row.get("status"), "src": row.get("src"),
            "nba_person_id": row.get("nba_person_id"),
        }, compare, at)
    for group in open_rows.values():
        for cur in group:
            if cur["id"] not in seen:
                _close(conn, "nba_lineup_states", cur["id"], at)
                st["closed"] += 1
    return st


def apply_goalies(conn, rows, at, close_missing=True) -> dict:
    """Change-only replace of one game's starters.

    Open rows are scoped to the same ESPN event, so a historical row for
    another game (or a row with no event) is left alone. A team this fetch
    could not read is not closed when ``close_missing`` is false.
    """
    compare = ("athlete_id", "goalie_name", "strength", "news_details", "espn_event_id", "source")
    st = {"new": 0, "changed": 0, "closed": 0, "same": 0}
    by_event = {}
    for row in rows:
        if row.get("team_id") is None or not row.get("game_date") or row.get("espn_event_id") is None:
            continue
        by_event.setdefault((row["game_date"], row["espn_event_id"]), []).append(row)
    for (day, eid), group in by_event.items():
        open_rows = {}
        for rec in conn.execute(
                "SELECT id, team_id, athlete_id, goalie_name, strength, news_details, "
                "espn_event_id, source FROM nhl_goalie_states "
                "WHERE espn_event_id = ? AND valid_to IS NULL", (eid,)):
            open_rows.setdefault(rec[1], []).append({
                "id": rec[0], "athlete_id": rec[2], "goalie_name": rec[3],
                "strength": rec[4], "news_details": rec[5], "espn_event_id": rec[6],
                "source": rec[7],
            })
        seen = set()
        for row in group:
            cur = next((r for r in open_rows.get(row["team_id"], []) if r["id"] not in seen), None)
            if cur and row.get("athlete_id") is None and cur.get("athlete_id") and _eq(cur["goalie_name"], row.get("goalie_name")):
                row["athlete_id"] = cur["athlete_id"]
            if cur and all(_eq(cur[k], row.get(k)) for k in compare):
                seen.add(cur["id"])
                st["same"] += 1
                continue
            if cur:
                _close(conn, "nhl_goalie_states", cur["id"], at)
                seen.add(cur["id"])
                st["changed"] += 1
            else:
                st["new"] += 1
            _insert(conn, "nhl_goalie_states", {
                "league": "nhl", "game_date": day, "dfo_team_id": int(row.get("dfo_team_id") or 0),
                "team_id": row["team_id"], "espn_event_id": eid,
                "goalie_name": row.get("goalie_name"), "athlete_id": row.get("athlete_id"),
                "strength": row.get("strength"), "news_details": row.get("news_details"),
                "source": row.get("source") or NHL_SOURCE,
                "nhl_player_id": row.get("nhl_player_id"),
            }, compare, at)
        if close_missing:
            for group_rows in open_rows.values():
                for cur in group_rows:
                    if cur["id"] not in seen:
                        _close(conn, "nhl_goalie_states", cur["id"], at)
                        st["closed"] += 1
    return st


def apply_lines(conn, team_id, rows, at, espn_event_id=None) -> dict:
    """Change-only replace of one team's lines for one game.

    ``espn_event_id`` None touches only legacy rows that have no event, which
    is how older line rows were stored. A game's lines never close those.
    """
    compare = ("athlete_id", "name", "position_id", "injury_status", "category", "source")
    if espn_event_id is None:
        where, args = "team_id = ? AND espn_event_id IS NULL AND valid_to IS NULL", (team_id,)
    else:
        where, args = "team_id = ? AND espn_event_id = ? AND valid_to IS NULL", (team_id, espn_event_id)
    open_rows = []
    for rec in conn.execute(
            "SELECT id, group_id, athlete_id, name, position_id, injury_status, category, source "
            f"FROM nhl_line_states WHERE {where}", args):
        open_rows.append({
            "id": rec[0], "group_id": rec[1], "athlete_id": rec[2], "name": rec[3],
            "position_id": rec[4], "injury_status": rec[5], "category": rec[6],
            "source": rec[7],
        })
    st = {"new": 0, "changed": 0, "closed": 0, "same": 0}
    seen = set()

    def find(row):
        for cur in open_rows:
            if cur["id"] in seen or cur["group_id"] != row["group_id"]:
                continue
            if row.get("athlete_id") and _eq(cur["athlete_id"], row["athlete_id"]):
                return cur
        if row.get("athlete_id"):
            return None
        for cur in open_rows:
            if cur["id"] in seen or cur["group_id"] != row["group_id"]:
                continue
            if norm_name(cur["name"]) == norm_name(row.get("name")):
                return cur
        return None

    for row in rows:
        cur = find(row)
        # Keep a published ESPN id when this page's name did not match.
        if cur and row.get("athlete_id") is None and cur.get("athlete_id"):
            row["athlete_id"] = cur["athlete_id"]
        if cur and all(_eq(cur[k], row.get(k)) for k in compare):
            seen.add(cur["id"])
            st["same"] += 1
            continue
        if cur:
            _close(conn, "nhl_line_states", cur["id"], at)
            seen.add(cur["id"])
            st["changed"] += 1
        else:
            st["new"] += 1
        _insert(conn, "nhl_line_states", {
            "league": "nhl", "dfo_team_id": int(row.get("dfo_team_id") or 0),
            "team_id": team_id, "group_id": row["group_id"], "category": row.get("category"),
            "dfo_player_id": int(row.get("dfo_player_id") or 0),
            "athlete_id": row.get("athlete_id"), "name": row.get("name"),
            "position_id": row.get("position_id"), "injury_status": row.get("injury_status"),
            "gtd": row.get("gtd") or 0, "source": row.get("source") or NHL_SOURCE,
            "espn_event_id": espn_event_id, "game_date": row.get("game_date"),
            "nhl_player_id": row.get("nhl_player_id"), "nhl_game_id": row.get("nhl_game_id"),
        }, ("athlete_id",), at)
    for cur in open_rows:
        if cur["id"] not in seen:
            _close(conn, "nhl_line_states", cur["id"], at)
            st["closed"] += 1
    return st


def _add(total, part):
    for key, value in part.items():
        total[key] = total.get(key, 0) + value
    return total


def _over(deadline):
    return time.perf_counter() > deadline


def _tx(conn):
    from .prelog import Tx
    return Tx(conn)


def _store_nba_side(conn, names, eid, tid, side_rows, at, st):
    slots = {}
    for row in side_rows:
        slots.setdefault(row["slot"], row)
    if len(slots) != 5:
        st["incomplete"] = st.get("incomplete", 0) + 1
        return False
    stored = []
    for slot, row in slots.items():
        aid = names.match(row["name"], tid)
        if aid is None:
            st["unmatched"] += 1
        stored.append({
            "slot": slot, "athlete_id": aid, "name": row["name"],
            "status": row["status"], "src": "nba.com",
            "nba_person_id": row["nba_person_id"],
        })
    with _tx(conn):
        _add(st, apply_nba_team(conn, eid, tid, stored, at))
    return True


def refresh_nba(conn, client, today, now, log, deadline) -> dict:
    start, end = _window(today)
    days = _dates(conn, "nba", start, end)
    st = {"errors": 0, "not_posted": 0, "teams": 0, "unmatched": 0}
    if not days:
        st["skipped"] = "no NBA games in the window"
        return st
    at = _stamp(now)
    teams, names = Teams(conn, "nba", NBA_ABBR), Names(conn, "nba")
    done = set()
    for day in days:
        if _over(deadline):
            st["budget"] = 1
            log("nba lineups stopped at the time budget; remaining dates wait for the next run")
            break
        url = NBA_URL.format(d=day.replace("-", ""))
        try:
            payload = json.loads(client.json(url))
            slate = parse_nba(payload)
        except Missing:
            st["not_posted"] += 1
            log(f"nba lineups {day} not posted")
            continue
        except Exception as e:
            st["errors"] += 1
            log(f"nba lineups {day} ERROR {type(e).__name__}: {e}")
            continue
        try:
            slate_day = datetime.fromisoformat(day).date()
        except ValueError:
            st["errors"] += 1
            log(f"nba lineups {day} ERROR ValueError: bad date")
            continue
        for game in slate:
            home_rows, away_rows = game.get("home") or [], game.get("away") or []
            if not home_rows or not away_rows:
                continue
            home = teams.by(abbr=game.get("home_abbr"))
            away = teams.by(abbr=game.get("away_abbr"))
            if home is None or away is None:
                st["unmatched"] += 1
                log(f"nba lineups {day} unmatched teams {game.get('away_abbr')}/{game.get('home_abbr')}")
                continue
            eid = _nba_event(conn, home, away, slate_day)
            if eid is None:
                st["unmatched"] += 1
                log(f"nba lineups {day} no ESPN game for {game.get('away_abbr')} at {game.get('home_abbr')}")
                continue
            for tid, side_rows in ((home, home_rows), (away, away_rows)):
                try:
                    if _store_nba_side(conn, names, eid, tid, side_rows, at, st):
                        done.add((eid, tid))
                        st["teams"] += 1
                except Exception as e:
                    st["errors"] += 1
                    log(f"nba lineups {day} team {tid} ERROR {type(e).__name__}: {e}")
    _depth(conn, client, today, at, teams, done, st, log, deadline)
    return st


def _depth(conn, client, today, at, teams, done, st, log, deadline):
    """ESPN depth chart for an upcoming team NBA.com did not list."""
    end = (today + timedelta(days=LOOKAHEAD_DAYS)).isoformat()
    rows = conn.execute(
        "SELECT espn_event_id, home_team_id, away_team_id FROM games "
        "WHERE league = 'nba' AND season_type IN (2, 3) "
        f"AND {db.playable_sql()} AND game_date BETWEEN ? AND ?",
        (today.isoformat(), end))
    for eid, home, away in rows:
        for tid in (home, away):
            if (eid, tid) in done or _over(deadline):
                continue
            have = conn.execute(
                "SELECT src FROM nba_lineup_states WHERE espn_event_id = ? AND team_id = ? "
                "AND valid_to IS NULL LIMIT 1", (eid, tid)).fetchone()
            if have and have[0] == "nba.com":
                continue
            espn_id = teams.espn.get(tid)
            if not espn_id:
                continue
            try:
                five = depth_starters(json.loads(client.json(DEPTH_URL.format(espn_id=espn_id))))
            except Missing:
                continue
            except Exception as e:
                st["errors"] += 1
                log(f"nba lineups depth team {tid} ERROR {type(e).__name__}: {e}")
                continue
            if len(five) != 5:
                st["depth_incomplete"] = st.get("depth_incomplete", 0) + 1
                continue
            stored = [{
                "slot": slot, "athlete_id": aid, "name": name, "status": "depth chart",
                "src": "espn_depth", "nba_person_id": None,
            } for slot, aid, name in five]
            try:
                with _tx(conn):
                    _add(st, apply_nba_team(conn, eid, tid, stored, at))
                st["teams_depth"] = st.get("teams_depth", 0) + 1
            except Exception as e:
                st["errors"] += 1
                log(f"nba lineups depth team {tid} ERROR {type(e).__name__}: {e}")


def _blank():
    return {"errors": 0, "unmatched": 0, "new": 0, "changed": 0, "closed": 0, "same": 0}


def _pending_nhl(conn, start, end):
    """Completed games in the window that do not yet have both official starters."""
    done = set()
    for eid, n in conn.execute(
            "SELECT espn_event_id, COUNT(DISTINCT team_id) FROM nhl_goalie_states "
            "WHERE source = ? AND valid_to IS NULL AND espn_event_id IS NOT NULL "
            "GROUP BY espn_event_id", (NHL_SOURCE,)):
        if n >= 2:
            done.add(eid)
    rows = conn.execute(
        "SELECT espn_event_id, game_date, home_team_id, away_team_id, start_time_utc "
        "FROM games WHERE league = 'nhl' AND season_type IN (2, 3) "
        f"AND {db.playable_sql()} AND completed = 1 AND game_date BETWEEN ? AND ? "
        "ORDER BY game_date, start_time_utc",
        (start.isoformat(), end.isoformat()))
    out = []
    for eid, day, home, away, start_utc in rows:
        if eid in done:
            continue
        out.append({
            "espn_event_id": eid, "game_date": str(day)[:10],
            "home_id": home, "away_id": away, "start": start_utc,
        })
    return out


def _when(stamp):
    if not stamp:
        return None
    try:
        got = datetime.fromisoformat(str(stamp).replace("Z", "+00:00"))
    except ValueError:
        return None
    if got.tzinfo is None:
        got = got.replace(tzinfo=timezone.utc)
    return got


def _pick_game(pending, indexed):
    """Same home and away. Prefer the schedule date, then the closer start."""
    day = datetime.fromisoformat(pending["game_date"]).date()
    want = _when(pending["start"])
    best = None
    for game in indexed:
        if game.get("home_id") != pending["home_id"] or game.get("away_id") != pending["away_id"]:
            continue
        if game.get("nhl_id") is None:
            continue
        try:
            gday = datetime.fromisoformat(str(game["date"])[:10]).date()
        except (TypeError, ValueError):
            continue
        dist = abs((gday - day).days)
        if dist > 1:
            continue
        start = _when(game.get("start"))
        delta = abs((start - want).total_seconds()) if start and want else 0
        rank = (dist, delta, game["nhl_id"])
        if best is None or rank < best[0]:
            best = (rank, game)
    return best[1] if best else None


def _shift_key(row):
    return (row.get("playerId"), row.get("period"), row.get("shiftNumber"),
            row.get("startTime"), row.get("teamAbbrev"))


def _fetch_shifts(client, gid):
    """All shift rows. The first response usually holds the game; page if it does not."""
    url = NHL_SHIFTS.format(gid=gid)
    payload = _loads(client.json(url))
    rows = shift_rows(payload)
    total = payload.get("total") if isinstance(payload, dict) else None
    total = total if isinstance(total, int) else len(rows)
    seen = {_shift_key(row) for row in rows}
    pages = 0
    while len(rows) < total and pages < 20:
        pages += 1
        page = _loads(client.json(f"{url}&start={len(rows)}&limit=500"))
        chunk = shift_rows(page)
        fresh = [row for row in chunk if _shift_key(row) not in seen]
        if not fresh:
            break
        for row in fresh:
            seen.add(_shift_key(row))
        rows.extend(fresh)
    return rows


def _load_schedule(client, dates, deadline, log, errors):
    """Games from the weeks that cover ``dates``. One request fills seven dates."""
    known, games = set(), []
    for day in dates:
        if day in known:
            continue
        if _over(deadline):
            errors["budget"] = 1
            log("nhl schedule stopped at the time budget; remaining dates wait for the next run")
            break
        try:
            payload = _loads(client.json(NHL_SCHEDULE.format(day=day)))
        except Missing as e:
            errors["errors"] += 1
            log(f"nhl schedule {day} ERROR Missing: {e}")
            continue
        except Exception as e:
            errors["errors"] += 1
            log(f"nhl schedule {day} ERROR {type(e).__name__}: {e}")
            continue
        week = [block.get("date") for block in (payload or {}).get("gameWeek") or [] if block.get("date")]
        for stamp in week or [day]:
            known.add(stamp)
        games.extend(iter_schedule(payload))
    return games


def _game_rows(box, shifts, names, side_ids, eid, day, nhl_id):
    """``side_ids`` maps the official team abbreviation to our team id."""
    roster = box_roster(box)
    full = shift_names(shifts)
    units = build_units(shifts, roster)
    goalie_rows, line_rows = [], []
    missed = False
    for side in ("awayTeam", "homeTeam"):
        abbr = (box.get(side) or {}).get("abbrev")
        team_id = side_ids.get(abbr)
        if team_id is None:
            missed = True
            continue
        starters = [
            (pid, info) for pid, info in roster.items()
            if info.get("starter") and info.get("kind") == "G" and info.get("team_abbr") == abbr
        ]
        if not starters:
            missed = True
            continue
        pid, info = starters[0]
        goalie_name = full.get(pid) or info.get("name")
        aid = _athlete(names, team_id, goalie_name, info.get("name"))
        goalie_rows.append({
            "game_date": day, "dfo_team_id": 0, "team_id": team_id, "espn_event_id": eid,
            "goalie_name": goalie_name, "athlete_id": aid, "strength": "Confirmed",
            "news_details": "official starter", "source": NHL_SOURCE, "nhl_player_id": pid,
            "_unmatched": bool(goalie_name) and aid is None,
        })
        for kind in ("lines", "pairs"):
            for unit in (units.get(abbr) or {}).get(kind) or []:
                for player in unit["players"]:
                    player_name = player.get("name")
                    aid = _athlete(names, team_id, player_name, None)
                    line_rows.append({
                        "team_id": team_id, "group_id": unit["group_id"], "category": "ev",
                        "dfo_team_id": 0, "dfo_player_id": player.get("player_id") or 0,
                        "nhl_player_id": player.get("player_id"), "name": player_name,
                        "position_id": player.get("position_id"), "injury_status": None, "gtd": 0,
                        "athlete_id": aid, "source": NHL_SOURCE, "game_date": day,
                        "nhl_game_id": nhl_id,
                        "_unmatched": bool(player_name) and aid is None,
                    })
    return goalie_rows, line_rows, missed


def refresh_nhl(conn, client, today, log, now, deadline):
    """Official starters, forward lines, and defense pairs for new final games."""
    start, end = _window(today)
    goalies, lines = _blank(), _blank()
    goalies["games"] = 0
    lines["teams"] = 0
    pending = _pending_nhl(conn, start, end)
    if not pending:
        note = "no unprocessed completed NHL games"
        goalies["skipped"] = note
        lines["skipped"] = note
        return goalies, lines
    at = _stamp(now)
    teams, roster_names = Teams(conn, "nhl", NHL_ABBR), Names(conn, "nhl")
    dates = sorted({row["game_date"] for row in pending})
    schedule = _load_schedule(client, dates, deadline, log, goalies)
    indexed = []
    for game in schedule:
        game = dict(game)
        game["home_id"] = teams.by(name=game.get("home_name"), abbr=game.get("home_abbr"))
        game["away_id"] = teams.by(name=game.get("away_name"), abbr=game.get("away_abbr"))
        indexed.append(game)
    for row in pending:
        if _over(deadline):
            goalies["budget"] = 1
            lines["budget"] = 1
            log("nhl official stopped at the time budget; remaining games wait for the next run")
            break
        label = f"{row['game_date']} event {row['espn_event_id']}"
        game = _pick_game(row, indexed)
        if game is None:
            goalies["unmatched"] += 1
            log(f"nhl official {label} unmatched schedule game")
            continue
        if game.get("state") not in NHL_FINAL:
            continue
        gid = game["nhl_id"]
        try:
            box = _loads(client.json(NHL_BOX.format(gid=gid)))
            if box.get("gameState") not in NHL_FINAL:
                continue
            if box.get("id") is not None and int(box["id"]) != int(gid):
                raise ValueError(f"boxscore id {box.get('id')} != {gid}")
            shifts = _fetch_shifts(client, gid)
            side_ids = {}
            if game.get("away_abbr"):
                side_ids[game["away_abbr"]] = row["away_id"]
            if game.get("home_abbr"):
                side_ids[game["home_abbr"]] = row["home_id"]
            goalie_rows, line_rows, missed = _game_rows(
                box, shifts, roster_names, side_ids, row["espn_event_id"], row["game_date"], gid)
        except Missing as e:
            goalies["errors"] += 1
            log(f"nhl official {label} ERROR Missing: {e}")
            continue
        except Exception as e:
            goalies["errors"] += 1
            log(f"nhl official {label} ERROR {type(e).__name__}: {e}")
            continue
        if not goalie_rows:
            goalies["errors"] += 1
            log(f"nhl official {label} ERROR ValueError: no starters")
            continue
        for item in goalie_rows:
            if item.pop("_unmatched", False):
                goalies["unmatched"] += 1
        by_team = {}
        for item in line_rows:
            if item.pop("_unmatched", False):
                lines["unmatched"] += 1
            by_team.setdefault(item["team_id"], []).append(item)
        try:
            with _tx(conn):
                _add(goalies, apply_goalies(conn, goalie_rows, at, close_missing=not missed))
                for team_id, team_rows in by_team.items():
                    _add(lines, apply_lines(conn, team_id, team_rows, at, row["espn_event_id"]))
                    lines["teams"] += 1
            goalies["games"] += 1
        except Exception as e:
            goalies["errors"] += 1
            log(f"nhl official {label} write ERROR {type(e).__name__}: {e}")
    return goalies, lines


def refresh(conn, log=log_default, today=None, now=None, fetch=None, gap=None) -> dict:
    """Refresh the recent NBA and NHL lineup window. Does not raise.

    ``gap`` is ignored. The requests are sequential JSON reads with no pause.
    """
    try:
        if conn.isolation_level is not None:
            conn.commit()
            conn.isolation_level = None
        if today is None:
            from .export import CENTRAL
            today = datetime.now(CENTRAL).date()
        client = Client(get=fetch, log=log)
        deadline = time.perf_counter() + BUDGET_S
        nba = refresh_nba(conn, client, today, now, log, deadline)
        nhl_started = time.perf_counter()
        goalies, lines = refresh_nhl(conn, client, today, log, now, deadline)
        nhl_seconds = time.perf_counter() - nhl_started
        out = {
            "errors": nba.get("errors", 0) + goalies.get("errors", 0) + lines.get("errors", 0),
            "requests": client.n,
            "nhl_seconds": round(nhl_seconds, 3),
            "nba": nba, "goalies": goalies, "lines": lines,
        }
        unmatched = (nba.get("unmatched", 0) + goalies.get("unmatched", 0)
                     + lines.get("unmatched", 0))
        log("lineups "
            f"nba +{nba.get('new', 0)} ~{nba.get('changed', 0)} "
            f"not_posted={nba.get('not_posted', 0)} "
            f"goalies +{goalies.get('new', 0)} ~{goalies.get('changed', 0)} "
            f"games={goalies.get('games', 0)} "
            f"lines +{lines.get('new', 0)} ~{lines.get('changed', 0)} "
            f"-{lines.get('closed', 0)} teams={lines.get('teams', 0)} "
            f"unmatched={unmatched} errors={out['errors']} requests={client.n} "
            f"nhl_s={nhl_seconds:.2f}")
        return out
    except Exception as e:
        log(f"lineup refresh ERROR {type(e).__name__}: {e}")
        return {"errors": 1, "fatal": f"{type(e).__name__}: {e}", "requests": 0}
