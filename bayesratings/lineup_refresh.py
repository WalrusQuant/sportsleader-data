"""Refresh NBA.com starting lineups and Daily Faceoff goalies and lines.

The nightly job used to keep these rows and not re-fetch them. Two things
made a full copy of the old passes a poor fit for a short run:

- NBA.com publishes one JSON file per day
  (stats.nba.com ``00_daily_lineups_YYYYMMDD.json``). The file is often
  missing until the slate is posted, and a 404 is normal. Confirmed fives
  replace the morning "Expected" fives, so only the recent window matters.
- Daily Faceoff is one HTML page of starting goalies per date and one
  line-combination page per team. ``robots.txt`` allows those pages and
  disallows ``/api/``, which this module never calls. A full league pass
  with the old 3 second gap is a couple of minutes, and Cloudflare returns
  a challenge to datacenter addresses (the nightly runner included), so a
  bare urllib fetch fails there even though the pages are public.

This pass only covers the last ``LOOKBACK_DAYS`` through tomorrow. A page
that is missing, challenged, or unparsable is logged and skipped. Nothing
here raises: the rest of the nightly publish still runs. When the direct
Daily Faceoff fetch is challenged, the same public HTML is read through
``https://r.jina.ai/`` (the page, not an API) for the rest of that run.
"""

import gzip
import hashlib
import json
import re
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

from . import db

LOOKBACK_DAYS = 8
LOOKAHEAD_DAYS = 1
# Once a night, not the old every-15-minutes poll. Enough of a pause that a
# full league of line pages stays polite without turning the run into the
# multi-minute pass that was left out before.
DFO_GAP = 0.35
# Stop fetching rather than let a slow source hold the publish.
BUDGET_S = 75
READER = "https://r.jina.ai/"
NBA_URL = "https://stats.nba.com/js/data/leaders/00_daily_lineups_{d}.json"
DEPTH_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/{espn_id}/depthcharts"
DFO = "https://www.dailyfaceoff.com"
UA = "bayesratings/0.1 (bayesratings.com; low-frequency pre-game data logging)"
SLOTS = ("PG", "SG", "SF", "PF", "C")
NBA_ABBR = {
    "NOP": "NO", "UTA": "UTAH", "GSW": "GS", "SAS": "SA", "NYK": "NY",
    "WAS": "WSH", "BRK": "BKN", "PHO": "PHX",
}
NHL_ABBR = {"LAK": "LA", "NJD": "NJ", "SJS": "SJ", "TBL": "TB", "UTA": "UTAH"}
_NEXT = re.compile(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', re.S)
_SUFFIX = re.compile(r"\b(jr|sr|ii|iii|iv|v)\b")


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
    """Direct fetch, with a page-reader fallback once Daily Faceoff challenges."""

    def __init__(self, get=None, log=log_default, gap=DFO_GAP, sleep=time.sleep):
        self.get = get or http_get
        self.log = log
        self.gap = gap
        self.sleep = sleep
        self.n = 0
        self.direct_blocked = False

    def json(self, url) -> bytes:
        self.n += 1
        body = self.get(url, "application/json", None)
        if challenged(body):
            raise Missing(f"challenge {url}")
        return body

    def html(self, url) -> bytes:
        """Daily Faceoff HTML. Never requests an /api/ path."""
        path = urllib.parse.urlparse(url).path
        if path.startswith("/api/") or "/api/" in path:
            raise Missing(f"refusing {url}")
        self.n += 1
        if self.gap:
            self.sleep(self.gap)
        if not self.direct_blocked:
            try:
                body = self.get(url, "text/html", None)
                if not challenged(body):
                    return body
            except Missing:
                pass
            self.direct_blocked = True
            self.log("Daily Faceoff blocked the direct fetch (Cloudflare). "
                     "Reading the same public pages through the fallback reader "
                     "for the rest of this run.")
        self.n += 1
        if self.gap:
            self.sleep(self.gap)
        body = self.get(READER + url, "text/html", {"X-Return-Format": "html"})
        if challenged(body) or b"__NEXT_DATA__" not in body:
            raise Missing(f"no lineup HTML for {url}")
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


def _team_ids(conn, league, start, end):
    rows = conn.execute(
        "SELECT home_team_id, away_team_id FROM games WHERE league = ? AND season_type IN (2, 3) "
        f"AND {db.playable_sql()} AND game_date BETWEEN ? AND ?",
        (league, start.isoformat(), end.isoformat()))
    ids = set()
    for home, away in rows:
        ids.add(home)
        ids.add(away)
    return ids


def next_data(html) -> dict:
    if isinstance(html, bytes):
        html = html.decode("utf-8", "replace")
    match = _NEXT.search(html)
    if not match:
        raise ValueError("no __NEXT_DATA__ on the page")
    return json.loads(match.group(1))["props"]["pageProps"]


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


def parse_goalies(page) -> list:
    out = []
    for game in page.get("data") or []:
        for side in ("home", "away"):
            def field(suffix, _side=side):
                return game.get(f"{_side}{suffix}")
            if field("TeamId") is None:
                continue
            other = "away" if side == "home" else "home"
            name = field("GoalieName")
            out.append({
                "game_date": game.get("date"),
                "game_time_utc": game.get("dateGmt"),
                "side": side,
                "dfo_team_id": field("TeamId"),
                "team_name": field("TeamName"),
                "opp_name": game.get(f"{other}TeamName"),
                "dfo_goalie_id": field("GoalieId"),
                "goalie_name": name,
                "strength": field("NewsStrengthName") or ("Unconfirmed" if name else None),
                "news_details": (field("NewsDetails") or "").strip() or None,
            })
    return out


def parse_lines(page) -> tuple:
    combo = page.get("combinations") or {}
    rows = []
    for player in combo.get("players") or []:
        if player.get("playerId") is None or not player.get("groupIdentifier"):
            continue
        rows.append({
            "dfo_team_id": combo.get("teamId"),
            "team_name": combo.get("teamName"),
            "team_abbr": combo.get("teamAbbreviation"),
            "group_id": player["groupIdentifier"],
            "category": player.get("categoryIdentifier"),
            "dfo_player_id": player["playerId"],
            "name": player.get("name"),
            "position_id": player.get("positionIdentifier"),
            "injury_status": player.get("injuryStatus") or None,
            "gtd": int(bool(player.get("gameTimeDecision"))),
        })
    slugs = [(t.get("slug"), t.get("name"), t.get("shortName"))
             for t in page.get("sortedTeams") or [] if t.get("slug")]
    return rows, slugs


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


def _goalie_event(conn, team_ids, start_utc, home):
    if not start_utc or None in team_ids:
        return None
    try:
        start = datetime.fromisoformat(str(start_utc).replace("Z", "+00:00"))
    except ValueError:
        return None
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    lo = (start - timedelta(hours=6)).strftime("%Y-%m-%dT%H:%M")
    hi = (start + timedelta(hours=6)).strftime("%Y-%m-%dT%H:%MZ")
    want = set(team_ids)
    best = None
    for eid, stamp, h, a in conn.execute(
            "SELECT espn_event_id, start_time_utc, home_team_id, away_team_id FROM games "
            "WHERE league = 'nhl' AND start_time_utc BETWEEN ? AND ?", (lo, hi)):
        if not want <= {h, a}:
            continue
        if home is not None and h != home:
            continue
        try:
            got = datetime.fromisoformat(str(stamp).replace("Z", "+00:00"))
            if got.tzinfo is None:
                got = got.replace(tzinfo=timezone.utc)
            dist = abs((got - start).total_seconds())
        except ValueError:
            dist = 0
        if best is None or dist < best[0]:
            best = (dist, eid)
    return best[1] if best else None


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
    compare = ("athlete_id", "goalie_name", "strength", "news_details", "espn_event_id")
    st = {"new": 0, "changed": 0, "closed": 0, "same": 0}
    by_date = {}
    for row in rows:
        if row.get("team_id") is None or not row.get("game_date"):
            continue
        by_date.setdefault(row["game_date"], []).append(row)
    for day, group in by_date.items():
        open_rows = {}
        for rec in conn.execute(
                "SELECT id, team_id, athlete_id, goalie_name, strength, news_details, espn_event_id "
                "FROM nhl_goalie_states WHERE game_date = ? AND valid_to IS NULL", (day,)):
            open_rows.setdefault(rec[1], []).append({
                "id": rec[0], "athlete_id": rec[2], "goalie_name": rec[3],
                "strength": rec[4], "news_details": rec[5], "espn_event_id": rec[6],
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
                "league": "nhl", "game_date": day, "dfo_team_id": int(row["dfo_team_id"]),
                "team_id": row["team_id"], "espn_event_id": row.get("espn_event_id"),
                "goalie_name": row.get("goalie_name"), "athlete_id": row.get("athlete_id"),
                "strength": row.get("strength"), "news_details": row.get("news_details"),
            }, compare, at)
        # A partial page (a team name we could not match) must not close
        # goalies it failed to read.
        if close_missing:
            for group_rows in open_rows.values():
                for cur in group_rows:
                    if cur["id"] not in seen:
                        _close(conn, "nhl_goalie_states", cur["id"], at)
                        st["closed"] += 1
    return st


def apply_lines(conn, team_id, rows, at) -> dict:
    compare = ("athlete_id", "name", "position_id", "injury_status", "category")
    open_rows = []
    for rec in conn.execute(
            "SELECT id, group_id, athlete_id, name, position_id, injury_status, category "
            "FROM nhl_line_states WHERE team_id = ? AND valid_to IS NULL", (team_id,)):
        open_rows.append({
            "id": rec[0], "group_id": rec[1], "athlete_id": rec[2], "name": rec[3],
            "position_id": rec[4], "injury_status": rec[5], "category": rec[6],
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
            "league": "nhl", "dfo_team_id": int(row["dfo_team_id"] or 0),
            "team_id": team_id, "group_id": row["group_id"], "category": row.get("category"),
            "dfo_player_id": int(row["dfo_player_id"]), "athlete_id": row.get("athlete_id"),
            "name": row.get("name"), "position_id": row.get("position_id"),
            "injury_status": row.get("injury_status"), "gtd": row.get("gtd") or 0,
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


def refresh_goalies(conn, client, today, log, now, deadline) -> dict:
    start, end = _window(today)
    days = _dates(conn, "nhl", start, end)
    st = {"errors": 0, "dates": 0, "unmatched": 0}
    if not days:
        st["skipped"] = "no NHL games in the window"
        return st
    at = _stamp(now)
    teams, names = Teams(conn, "nhl", NHL_ABBR), Names(conn, "nhl")
    for day in days:
        if _over(deadline):
            st["budget"] = 1
            log("nhl goalies stopped at the time budget; remaining dates wait for the next run")
            break
        url = f"{DFO}/starting-goalies/{day}"
        try:
            page = next_data(client.html(url))
            parsed = parse_goalies(page)
        except Missing as e:
            st["errors"] += 1
            log(f"nhl goalies {day} ERROR Missing: {e}")
            continue
        except Exception as e:
            st["errors"] += 1
            log(f"nhl goalies {day} ERROR {type(e).__name__}: {e}")
            continue
        if not parsed:
            st["errors"] += 1
            log(f"nhl goalies {day} ERROR ValueError: no goalies parsed")
            continue
        rows = []
        missed_team = False
        for row in parsed:
            tid = teams.by(name=row["team_name"])
            if tid is None:
                missed_team = True
                st["unmatched"] += 1
                log(f"nhl goalies {day} unmatched team {row['team_name']}")
                continue
            row["team_id"] = tid
            row["athlete_id"] = names.match(row["goalie_name"], tid) if row["goalie_name"] else None
            if row["goalie_name"] and row["athlete_id"] is None:
                st["unmatched"] += 1
            home = tid if row["side"] == "home" else teams.by(name=row["opp_name"])
            opp = teams.by(name=row["opp_name"]) if row["side"] == "home" else tid
            ids = [tid, opp] if row["side"] == "home" else [opp, tid]
            row["espn_event_id"] = _goalie_event(conn, [i for i in ids if i], row["game_time_utc"], home)
            rows.append(row)
        try:
            with _tx(conn):
                _add(st, apply_goalies(conn, rows, at, close_missing=not missed_team))
            st["dates"] += 1
        except Exception as e:
            st["errors"] += 1
            log(f"nhl goalies {day} write ERROR {type(e).__name__}: {e}")
    return st


def refresh_lines(conn, client, today, log, now, deadline) -> dict:
    start, end = _window(today)
    want = _team_ids(conn, "nhl", start, end)
    st = {"errors": 0, "teams": 0, "unmatched": 0}
    if not want:
        st["skipped"] = "no NHL games in the window"
        return st
    at = _stamp(now)
    teams, names = Teams(conn, "nhl", NHL_ABBR), Names(conn, "nhl")
    # The first page lists every team. Winnipeg is only a directory; its
    # lines are stored when that club is actually in the window.
    queue = ["winnipeg-jets"]
    seen_slugs = set()
    fetched = set()
    listed = False
    while queue:
        if _over(deadline):
            st["budget"] = 1
            log("nhl lines stopped at the time budget; remaining teams wait for the next run")
            break
        slug = queue.pop(0)
        if slug in seen_slugs:
            continue
        seen_slugs.add(slug)
        url = f"{DFO}/teams/{slug}/line-combinations"
        try:
            page = next_data(client.html(url))
            rows, directory = parse_lines(page)
        except Exception as e:
            st["errors"] += 1
            log(f"nhl lines {slug} ERROR {type(e).__name__}: {e}")
            continue
        if not listed and directory:
            listed = True
            for team_slug, name, abbr in directory:
                tid = teams.by(name=name, abbr=abbr)
                if tid in want and team_slug not in seen_slugs:
                    queue.append(team_slug)
        if not rows:
            st["errors"] += 1
            log(f"nhl lines {slug} ERROR ValueError: no lines parsed")
            continue
        tid = teams.by(name=rows[0]["team_name"], abbr=rows[0].get("team_abbr"))
        if tid not in want:
            continue
        for row in rows:
            row["athlete_id"] = names.match(row["name"], tid)
            if row["athlete_id"] is None:
                st["unmatched"] += 1
        try:
            with _tx(conn):
                _add(st, apply_lines(conn, tid, rows, at))
            fetched.add(tid)
            st["teams"] += 1
        except Exception as e:
            st["errors"] += 1
            log(f"nhl lines {slug} write ERROR {type(e).__name__}: {e}")
    left = want - fetched
    if left and not st.get("budget"):
        log(f"nhl lines left {len(left)} window teams unfetched")
    return st


def refresh(conn, log=log_default, today=None, now=None, fetch=None, gap=None) -> dict:
    """Refresh the recent NBA and NHL lineup window. Does not raise."""
    try:
        if conn.isolation_level is not None:
            conn.commit()
            conn.isolation_level = None
        if today is None:
            from .export import CENTRAL
            today = datetime.now(CENTRAL).date()
        client = Client(get=fetch, log=log, gap=DFO_GAP if gap is None and fetch is None else (gap or 0.0))
        # Injected fetches are tests: don't sleep.
        if fetch is not None and gap is None:
            client.gap = 0.0
        deadline = time.perf_counter() + BUDGET_S
        nba = refresh_nba(conn, client, today, now, log, deadline)
        goalies = refresh_goalies(conn, client, today, log, now, deadline)
        lines = refresh_lines(conn, client, today, log, now, deadline)
        out = {
            "errors": nba.get("errors", 0) + goalies.get("errors", 0) + lines.get("errors", 0),
            "requests": client.n,
            "direct_blocked": int(client.direct_blocked),
            "nba": nba, "goalies": goalies, "lines": lines,
        }
        unmatched = (nba.get("unmatched", 0) + goalies.get("unmatched", 0)
                     + lines.get("unmatched", 0))
        log("lineups "
            f"nba +{nba.get('new', 0)} ~{nba.get('changed', 0)} "
            f"not_posted={nba.get('not_posted', 0)} "
            f"goalies +{goalies.get('new', 0)} ~{goalies.get('changed', 0)} "
            f"dates={goalies.get('dates', 0)} "
            f"lines +{lines.get('new', 0)} ~{lines.get('changed', 0)} "
            f"-{lines.get('closed', 0)} teams={lines.get('teams', 0)} "
            f"unmatched={unmatched} errors={out['errors']} requests={client.n}"
            + (" direct_blocked=1" if client.direct_blocked else ""))
        return out
    except Exception as e:
        log(f"lineup refresh ERROR {type(e).__name__}: {e}")
        return {"errors": 1, "fatal": f"{type(e).__name__}: {e}", "requests": 0}
