# ESPN scoreboard client + parser for the four major leagues.
#
# Pure fetch + parse: this module hits the network and turns ESPN JSON into
# rows; it never touches the DB (db.ingest_scoreboard does that). Stdlib only —
# no requests dependency. Reference: docs/espn-reference/.

import gzip
import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

SITE_API = "https://site.api.espn.com/apis/site/v2/sports"
_EASTERN = ZoneInfo("America/New_York")
# The site's single clock: game_date (the slate key), "today" on every page and
# the daily/hourly jobs all use Central time. Eastern survives only where an
# outside system keys on it (ESPN's ?dates= buckets, Kalshi event tickers).
CENTRAL = ZoneInfo("America/Chicago")
IF_NECESSARY_RE = re.compile(r"if\s+necessary", re.I)

# Per-league config. season_convention documents the season.year meaning
# (docs/espn-reference/gotchas.md #11); the parser trusts ESPN's season.year as
# given, so this is reference for the backfill loop, not parsing logic.
LEAGUES = {
    "mlb": {"sport": "baseball",   "league": "mlb", "season_convention": "calendar"},
    "nfl": {"sport": "football",   "league": "nfl", "season_convention": "start"},
    "nba": {"sport": "basketball", "league": "nba", "season_convention": "end"},
    "nhl": {"sport": "hockey",     "league": "nhl", "season_convention": "end"},
}


# ---------------------------------------------------------------- fetch core

def _get(url: str, params: dict = None, retries: int = 4,
         timeout: int = 30) -> dict:
    """GET JSON with polite headers, gzip, and exponential backoff on 429/5xx."""
    if params:
        url = url + "?" + urllib.parse.urlencode(
            {k: v for k, v in params.items() if v is not None})
    req = urllib.request.Request(url, headers={
        "User-Agent": "bayesratings/0.1 (+games backfill; contact via repo)",
        "Accept": "application/json",
        "Accept-Encoding": "gzip",
    })
    delay = 1.0
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                if resp.headers.get("Content-Encoding") == "gzip":
                    raw = gzip.decompress(raw)
                return json.loads(raw.decode())
        except urllib.error.HTTPError as e:
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


def fetch_scoreboard(league: str, dates: str = None, week: int = None,
                     seasontype: int = None, limit: int = 300) -> dict:
    """Fetch a scoreboard payload. `dates` is YYYYMMDD or YYYYMMDD-YYYYMMDD;
    `week`/`seasontype` drive NFL week-based queries."""
    cfg = LEAGUES[league]
    url = f"{SITE_API}/{cfg['sport']}/{cfg['league']}/scoreboard"
    return _get(url, {"dates": dates, "week": week, "seasontype": seasontype,
                      "limit": limit})


def fetch_teams(league: str) -> dict:
    cfg = LEAGUES[league]
    return _get(f"{SITE_API}/{cfg['sport']}/{cfg['league']}/teams")


def fetch_summary(league: str, event_id: int) -> dict:
    """Fetch the completed-game box-score response for one ESPN event."""
    cfg = LEAGUES[league]
    return _get(f"{SITE_API}/{cfg['sport']}/{cfg['league']}/summary", {"event": event_id})


# -------------------------------------------------------------------- parse

def _utc(iso_utc: str) -> datetime:
    dt = datetime.fromisoformat(iso_utc.replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def eastern_date(iso_utc: str) -> str:
    """US/Eastern calendar date of an ESPN UTC start. ESPN's ?dates= scoreboard
    buckets and Kalshi's event tickers use this date; the site does not."""
    return _utc(iso_utc).astimezone(_EASTERN).date().isoformat()


def central_date(iso_utc: str) -> str:
    """America/Chicago calendar date of an ESPN UTC start: games.game_date,
    the same clock as the site's "today". A 10:10pm CT start is next-day in
    UTC (and an 11:30pm CT start is next-day in Eastern)."""
    return _utc(iso_utc).astimezone(CENTRAL).date().isoformat()


def _score(competitor: dict, completed: bool):
    """Scores are strings and only trustworthy when the game is completed."""
    if not completed:
        return None
    raw = competitor.get("score")
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _ending(league: str, status: dict, completed: bool):
    """NHL game ending: regulation | ot | shootout (None for other leagues or
    unfinished games). Shootouts show as 'Final/SO'; any period past the 3rd
    is overtime (playoff OT can run to period 4+, never a shootout)."""
    if league != "nhl" or not completed:
        return None
    detail = ((status.get("type") or {}).get("shortDetail") or "").upper()
    if "SO" in detail:
        return "shootout"
    period = status.get("period") or 0
    if period > 3 or "OT" in detail:
        return "ot"
    return "regulation"


def _team_display(team: dict) -> dict:
    """Pull display fields from a team object (scoreboard competitor or /teams)."""
    logos = team.get("logos")
    logo = team.get("logo") or (logos[0]["href"] if logos else None)
    return {
        "espn_id": int(team["id"]),
        "location": team.get("location"),
        "name": team.get("displayName"),
        "abbreviation": team.get("abbreviation"),
        "color": team.get("color"),
        "alternate_color": team.get("alternateColor"),
        "logo": logo,
    }


# Exhibition events ESPN files as regular season (season.type 2): All-Star
# games and skills events, NBA Rising Stars, NFL Pro Bowl, NHL 4 Nations
# Face-Off and other international/best-on-best tournaments.
EXHIBITION_RE = re.compile(
    r"all[- ]?star|pro bowl|rising stars|face[- ]?off|4 nations|four nations|"
    r"world cup of hockey|olympic|skills challenge|celebrity", re.I)


def is_exhibition(event: dict) -> bool:
    """True for All-Star / Pro Bowl / international exhibition events, judged
    from the competition type and the event name and notes."""
    comp = (event.get("competitions") or [{}])[0]
    if ((comp.get("type") or {}).get("abbreviation") or "").upper() == "ALLSTAR":
        return True
    text = [event.get("name"), event.get("shortName")]
    for src in (event, comp):
        for n in src.get("notes") or []:
            text.append(n.get("headline") if isinstance(n, dict) else n)
    return any(EXHIBITION_RE.search(t) for t in text if isinstance(t, str))


def parse_scoreboard(data: dict, league: str) -> dict:
    """Turn a scoreboard payload into {"teams": [...], "games": [...]}.
    Games carry home_espn_id/away_espn_id; db.ingest_scoreboard resolves those
    to canonical team_ids. Malformed events are skipped, not fatal."""
    teams = {}
    games = []
    for event in data.get("events", []):
        try:
            comps = event["competitions"]
            comp = comps[0]
            status = comp.get("status", {}).get("type", {})
            completed = bool(status.get("completed"))
            competitors = comp["competitors"]
            by_side = {c.get("homeAway"): c for c in competitors}
            home, away = by_side.get("home"), by_side.get("away")
            if home is None or away is None:
                continue

            for c in (home, away):
                td = _team_display(c["team"])
                teams[td["espn_id"]] = td

            season = event.get("season", {})
            venue = comp.get("venue") or {}
            vaddr = venue.get("address") or {}
            notes = [n.get("headline") if isinstance(n, dict) else n
                     for n in (comp.get("notes") or event.get("notes") or [])]
            notes = "; ".join(n for n in notes if isinstance(n, str) and n)
            series = comp.get("series") or {}
            games.append({
                "espn_event_id": int(event["id"]),
                "season": int(season.get("year")),
                "season_type": season.get("type"),
                "start_time_utc": event["date"],
                "game_date": central_date(event["date"]),
                "home_espn_id": int(home["team"]["id"]),
                "away_espn_id": int(away["team"]["id"]),
                "home_score": _score(home, completed),
                "away_score": _score(away, completed),
                "completed": completed,
                "status": status.get("name"),
                "venue": venue.get("fullName"),
                "venue_city": vaddr.get("city"),
                "venue_state": vaddr.get("state"),
                "venue_country": vaddr.get("country"),
                "neutral_site": bool(comp.get("neutralSite")),
                "comp_type": (comp.get("type") or {}).get("abbreviation"),
                "ending": _ending(league, comp.get("status", {}), completed),
                "exhibition": is_exhibition(event),
                "notes": notes or None,
                "if_necessary": bool(IF_NECESSARY_RE.search(notes)),
                "series_summary": series.get("summary"),
                "series_completed": bool(series.get("completed")),
            })
        except (KeyError, TypeError, ValueError):
            continue
    return {"teams": list(teams.values()), "games": games}


def parse_teams(data: dict) -> list:
    """Parse the /teams payload (sports[0].leagues[0].teams[].team)."""
    out = []
    for sport in data.get("sports", []):
        for lg in sport.get("leagues", []):
            for entry in lg.get("teams", []):
                team = entry.get("team")
                if team and team.get("id"):
                    out.append(_team_display(team))
    return out
