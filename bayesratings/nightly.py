"""Nightly update. Load the current-season release files, refresh the last
two days from ESPN, export, and upload only the files that changed.

    python -m bayesratings.nightly --dry-run

Rosters, depth charts, ESPN transactions, and new ESPN injury details are
refreshed from the copied prelog passes. Scoreboard probables and ESPN
weather move with the game ingest. NBA.com lineups and Daily Faceoff
goalies and lines are refreshed for the recent window; a failure there is
logged and does not stop the publish. See snapshots.NOT_REFRESHED for the
sources that are kept but not re-fetched.
"""

import argparse
import hashlib
import json
import math
import os
import sys
import time
import urllib.request

import numpy as np
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from . import backfill, db, export, gh_publish, labstats, players, snapshots, teambox
from .export import BOX, COUNT_NAMES, SEASON_TYPES

OWNER_REPO = "WalrusQuant/sportsleader-data"
RELEASE = f"https://github.com/{OWNER_REPO}/releases/download"
LEAGUES = ("mlb", "nba", "nfl", "nhl")
UPDATE = (
    "games", "team_box_scores", "team_game_stats",
    "player_game_logs", "skater_game_logs", "goalie_game_logs",
    "batting_game_logs", "pitching_game_logs",
    "passing_game_logs", "rushing_game_logs", "receiving_game_logs",
)
SNAPSHOTS = snapshots.DATASETS
LOG_TABLE = {
    "player_game_logs": "player_game_nba",
    "skater_game_logs": "player_game_nhl_skater",
    "goalie_game_logs": "player_game_nhl_goalie",
    "batting_game_logs": "player_game_mlb_batting",
    "pitching_game_logs": "player_game_mlb_pitching",
    "passing_game_logs": "player_game_nfl_passing",
    "rushing_game_logs": "player_game_nfl_rushing",
    "receiving_game_logs": "player_game_nfl_receiving",
}
# Parquet column -> database column. Bools are stored as 0/1.
LOG_RENAME = {
    "player_game_logs": {
        "starter": "starter", "did_not_play": "did_not_play", "dnp_reason": "dnp_reason",
        "ejected": "ejected", "minutes": "minutes", "points": "pts",
        "fg_made": "fgm", "fg_att": "fga", "fg3_made": "fg3m", "fg3_att": "fg3a",
        "ft_made": "ftm", "ft_att": "fta", "rebounds": "reb", "off_rebounds": "oreb",
        "def_rebounds": "dreb", "assists": "ast", "turnovers": "tov", "steals": "stl",
        "blocks": "blk", "fouls": "pf", "plus_minus": "plus_minus",
    },
    "skater_game_logs": {
        "unit": "unit", "toi_seconds": "toi_sec", "pp_toi_seconds": "pp_toi_sec",
        "sh_toi_seconds": "sh_toi_sec", "es_toi_seconds": "es_toi_sec", "shifts": "shifts",
        "goals": "g", "assists": "a", "shots": "shots", "shots_missed": "shots_missed",
        "plus_minus": "plus_minus", "hits": "hits", "blocked_shots": "blocks",
        "takeaways": "takeaways", "giveaways": "giveaways", "faceoffs_won": "fo_won",
        "faceoffs_lost": "fo_lost", "penalty_minutes": "pim", "shootout_goals": "so_goals",
    },
    "goalie_game_logs": {
        "starter": "starter", "goalie_order": "goalie_order", "toi_seconds": "toi_sec",
        "shots_against": "sa", "goals_against": "ga", "saves": "sv", "save_pct": "sv_pct",
        "even_strength_saves": "es_sv", "power_play_saves": "pp_sv",
        "short_handed_saves": "sh_sv", "shootout_shots_against": "so_sa",
        "shootout_saves": "so_sv", "penalty_minutes": "pim",
    },
    "batting_game_logs": {
        "starter": "starter", "batting_order": "bat_order", "at_bats": "ab", "runs": "r",
        "hits": "h", "rbi": "rbi", "home_runs": "hr", "walks": "bb", "strikeouts": "k",
        "pitches_seen": "pitches",
    },
    "pitching_game_logs": {
        "starter": "starter", "pitching_order": "pitch_order", "decision": "decision",
        "outs": "outs", "hits_allowed": "h", "runs_allowed": "r", "earned_runs": "er",
        "walks": "bb", "strikeouts": "k", "home_runs_allowed": "hr", "pitches": "pitches",
        "strikes": "strikes",
    },
    "passing_game_logs": {
        "starter": "qb_starter", "completions": "cmp", "attempts": "att", "yards": "yds",
        "touchdowns": "td", "interceptions": "ints", "sacks": "sacks",
        "sack_yards_lost": "sack_yds", "qbr": "qbr", "passer_rating": "rating",
    },
    "rushing_game_logs": {
        "carries": "car", "yards": "yds", "touchdowns": "td", "longest": "long",
    },
    "receiving_game_logs": {
        "receptions": "rec", "targets": "tgt", "yards": "yds", "touchdowns": "td",
        "longest": "long",
    },
}
BOOLS = {"starter", "did_not_play", "ejected", "qb_starter"}
STYPE = {name: num for num, name in SEASON_TYPES.items()}


def log(msg):
    print(msg, flush=True)


def empty(v):
    if v is None:
        return True
    try:
        return bool(pd.isna(v))
    except (TypeError, ValueError):
        return False


def num(v):
    if empty(v):
        return None
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, float) and math.isfinite(v) and v == int(v):
        return int(v)
    return v


def as_int(v, default=None):
    v = num(v)
    if v is None:
        return default
    return int(v)


def as_bool(v):
    if empty(v):
        return 0
    return int(bool(v))


def download(url, dest: Path):
    dest.parent.mkdir(parents=True, exist_ok=True)
    urllib.request.urlretrieve(url, dest)


def current_files(manifest):
    latest = {}
    for entry in manifest["files"]:
        latest[entry["league"]] = max(latest.get(entry["league"], 0), int(entry["season"]))
    keep = set(UPDATE) | set(SNAPSHOTS)
    wanted = []
    for entry in manifest["files"]:
        if entry["dataset"] in keep and int(entry["season"]) == latest[entry["league"]]:
            wanted.append(entry)
    return latest, wanted


class Teams:
    def __init__(self, conn):
        self.conn = conn
        self.pk = {}
        self.next_id = 1

    def id(self, league, espn_id, abbr=None, name=None):
        key = (league, int(espn_id))
        if key in self.pk:
            return self.pk[key]
        tid = self.next_id
        self.next_id += 1
        self.conn.execute(
            "INSERT INTO teams (team_id, league, espn_id, abbreviation, name) VALUES (?,?,?,?,?)",
            (tid, league, int(espn_id),
             None if empty(abbr) else str(abbr),
             None if empty(name) else str(name)))
        self.pk[key] = tid
        return tid


def load_games(conn, teams, frame):
    rows = []
    for rec in frame.itertuples(index=False):
        start = rec.start_time
        start = start.isoformat() if hasattr(start, "isoformat") else str(start)
        gdate = rec.game_date
        gdate = gdate.isoformat() if hasattr(gdate, "isoformat") else str(gdate)[:10]
        rows.append({
            "espn_event_id": int(rec.game_id),
            "league": rec.league,
            "season": int(rec.season),
            "season_type": STYPE.get(str(rec.season_type)),
            "start_time_utc": start,
            "game_date": gdate,
            "home_team_id": teams.id(rec.league, rec.home_team_id, rec.home_team_abbr, rec.home_team_name),
            "away_team_id": teams.id(rec.league, rec.away_team_id, rec.away_team_abbr, rec.away_team_name),
            "home_score": as_int(rec.home_score),
            "away_score": as_int(rec.away_score),
            "completed": int(bool(rec.completed)) if not empty(rec.completed) else 0,
            "status": None if empty(rec.status) else "STATUS_" + str(rec.status).upper(),
            "venue": None if empty(rec.venue) else str(rec.venue),
            "neutral_site": 0 if empty(rec.neutral_site) else int(bool(rec.neutral_site)),
            "ending": None if empty(rec.ending) else str(rec.ending),
            "covid_2020": 0,
            "notes": None if empty(rec.notes) else str(rec.notes),
            "if_necessary": 0 if empty(rec.if_necessary) else int(bool(rec.if_necessary)),
            "series_summary": None if empty(rec.series_summary) else str(rec.series_summary),
            "series_completed": 0,
            "venue_city": None if empty(rec.venue_city) else str(rec.venue_city),
            "venue_state": None if empty(rec.venue_state) else str(rec.venue_state),
            "venue_country": None if empty(rec.venue_country) else str(rec.venue_country),
        })
    conn.commit()
    db.upsert_games(conn, rows)
    return len(rows)


def load_box(conn, teams, league, frame):
    spec = BOX[league]
    rows = []
    version = teambox.version(league)
    for rec in frame.to_dict("records"):
        stats = []
        for (cat, name), out in spec:
            value = rec.get(out)
            stats.append([cat, name, None if empty(value) else num(value)])
        rows.append((
            int(rec["game_id"]),
            teams.id(league, rec["team_id"], rec.get("team_abbr"), rec.get("team_name")),
            league, "loaded", version,
            datetime.now(timezone.utc).isoformat(timespec="seconds"),
            json.dumps(stats),
        ))
    conn.executemany(
        "INSERT OR REPLACE INTO team_game_box (espn_event_id, team_id, league, "
        "summary_fetched_at, version, extracted_at, stats_json) VALUES (?,?,?,?,?,?,?)",
        rows)
    conn.commit()
    return len(rows)


def load_stats(conn, teams, league, frame):
    keys = labstats.COUNTS[league]
    names = [COUNT_NAMES[league][k] for k in keys]
    rows = []
    for rec in frame.to_dict("records"):
        mine = [None if empty(rec.get(n)) else num(rec.get(n)) for n in names]
        theirs = [None if empty(rec.get("opponent_" + n)) else num(rec.get("opponent_" + n))
                  for n in names]
        rows.append((
            int(rec["game_id"]),
            teams.id(league, rec["team_id"], rec.get("team_abbr"), rec.get("team_name")),
            teams.id(league, rec["opponent_id"], rec.get("opponent_abbr"), rec.get("opponent_name")),
            league, int(rec["season"]), STYPE.get(str(rec["season_type"])),
            rec["game_date"].isoformat() if hasattr(rec["game_date"], "isoformat") else str(rec["game_date"])[:10],
            0 if empty(rec["is_home"]) else int(bool(rec["is_home"])),
            as_int(rec.get("score")), as_int(rec.get("opponent_score")),
            "loaded", labstats.VERSION, json.dumps([mine, theirs]),
        ))
    conn.executemany(
        "INSERT OR REPLACE INTO team_game_stats (espn_event_id, team_id, opp_team_id, "
        "league, season, season_type, game_date, is_home, points_for, points_against, "
        "summary_fetched_at, version, counts_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        rows)
    conn.commit()
    return len(rows)


def load_log(conn, teams, league, dataset, frame):
    table = LOG_TABLE[dataset]
    rename = LOG_RENAME[dataset]
    cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]
    order = {}
    records = frame.to_dict("records")
    rows = []
    names = []
    for rec in records:
        key = (int(rec["game_id"]), int(rec["team_id"]))
        order[key] = order.get(key, 0) + 1
        mapped = {}
        for src, dst in rename.items():
            value = rec.get(src)
            if dst in BOOLS or src in BOOLS:
                mapped[dst] = as_bool(value)
            else:
                mapped[dst] = None if empty(value) else num(value)
        if "box_order" in cols:
            mapped["box_order"] = order[key]
        if "pitch_order" in cols and mapped.get("pitch_order") is None:
            mapped["pitch_order"] = order[key]
        if "goalie_order" in cols and mapped.get("goalie_order") is None:
            mapped["goalie_order"] = order[key]
        if "unit" in cols and not mapped.get("unit"):
            mapped["unit"] = "skaters"
        if "starter" in cols and mapped.get("starter") is None:
            mapped["starter"] = 0
        if "did_not_play" in cols and mapped.get("did_not_play") is None:
            mapped["did_not_play"] = 0
        if "qb_starter" in cols and mapped.get("qb_starter") is None:
            mapped["qb_starter"] = 0
        gdate = rec["game_date"]
        gdate = gdate.isoformat() if hasattr(gdate, "isoformat") else str(gdate)[:10]
        base = {
            "espn_event_id": int(rec["game_id"]),
            "season": int(rec["season"]),
            "season_type": STYPE.get(str(rec["season_type"])),
            "game_date": gdate,
            "team_id": teams.id(league, rec["team_id"], rec.get("team_abbr"), rec.get("team_name")),
            "opp_team_id": teams.id(league, rec["opponent_id"], rec.get("opponent_abbr"), rec.get("opponent_name")),
            "is_home": 0 if empty(rec["is_home"]) else int(bool(rec["is_home"])),
            "athlete_id": int(rec["player_id"]),
            "position": None if empty(rec.get("position")) else str(rec.get("position")),
            "stats_json": "{}",
        }
        base.update(mapped)
        rows.append(tuple(base.get(c) for c in cols))
        names.append((league, int(rec["player_id"]),
                      None if empty(rec.get("player_name")) else str(rec["player_name"]),
                      None if empty(rec.get("position")) else str(rec.get("position")),
                      base["team_id"], gdate))
    holders = ",".join("?" * len(cols))
    conn.executemany(
        f"INSERT OR REPLACE INTO {table} ({', '.join(cols)}) VALUES ({holders})", rows)
    conn.executemany(
        "INSERT INTO players (league, athlete_id, name, position, team_id, last_game_date) "
        "VALUES (?,?,?,?,?,?) ON CONFLICT (league, athlete_id) DO UPDATE SET "
        "name = COALESCE(excluded.name, players.name), "
        "position = COALESCE(excluded.position, players.position), "
        "last_game_date = MAX(COALESCE(players.last_game_date, ''), excluded.last_game_date)",
        names)
    conn.commit()
    return len(rows)


def load_file(conn, teams, entry, path):
    frame = pd.read_parquet(path)
    dataset = entry["dataset"]
    league = entry["league"]
    if dataset == "games":
        return load_games(conn, teams, frame)
    if dataset == "team_box_scores":
        return load_box(conn, teams, league, frame)
    if dataset == "team_game_stats":
        return load_stats(conn, teams, league, frame)
    if dataset in SNAPSHOTS:
        return snapshots.load(conn, teams, league, dataset, frame)
    return load_log(conn, teams, league, dataset, frame)


def canon(v):
    if isinstance(v, (bool, np.bool_)):
        return bool(v)
    if empty(v):
        return None
    if isinstance(v, pd.Timestamp):
        ts = v.tz_convert("UTC") if v.tzinfo else v.tz_localize("UTC")
        return ts.strftime("%Y-%m-%dT%H:%M:%SZ")
    if isinstance(v, np.integer):
        return int(v)
    if isinstance(v, (float, np.floating)):
        v = float(v)
        if not math.isfinite(v):
            return None
        if v == int(v) and abs(v) < 1e12:
            return int(v)
        return round(v, 4)
    if isinstance(v, int):
        return int(v)
    if hasattr(v, "isoformat") and not isinstance(v, str):
        return v.isoformat()
    text = str(v)
    return None if text in ("", "nan", "None", "NaT") else text


def row_diff(old_path, new_path):
    old = pd.read_parquet(old_path)
    new = pd.read_parquet(new_path)
    if list(old.columns) != list(new.columns):
        return {"rows_added": None, "rows_updated": None, "rows_removed": None,
                "column_mismatch": True, "old_rows": len(old), "new_rows": len(new)}
    cols = list(old.columns)

    def bags(frame):
        counts = {}
        for rec in frame.to_dict("records"):
            row = tuple(canon(rec[c]) for c in cols)
            counts[row] = counts.get(row, 0) + 1
        return counts

    a, b = bags(old), bags(new)
    added = removed = 0
    for row in set(a) | set(b):
        delta = b.get(row, 0) - a.get(row, 0)
        if delta > 0:
            added += delta
        elif delta < 0:
            removed += -delta
    return {"rows_added": added, "rows_updated": 0, "rows_removed": removed,
            "column_mismatch": False, "old_rows": len(old), "new_rows": len(new)}


def sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def file_entry(old, path, rel, diff):
    table = pq.read_table(path)
    return {
        **{k: old[k] for k in old if k not in ("rows", "bytes", "sha256", "updated_at", "url")},
        "path": rel,
        "rows": table.num_rows,
        "columns": old["columns"],
        "bytes": path.stat().st_size,
        "sha256": sha256(path),
        "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "url": gh_publish.asset_url(old["league"], old["dataset"], old["season"]),
        "_diff": diff,
    }


def with_urls(manifest):
    files = []
    for entry in manifest["files"]:
        item = dict(entry)
        item.pop("_diff", None)
        item["url"] = gh_publish.asset_url(item["league"], item["dataset"], item["season"])
        files.append(item)
    out = dict(manifest)
    out["files"] = files
    out["manifest_url"] = gh_publish.manifest_url()
    out["generated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    return out


def run(dry_run=False, cache=None):
    t0 = time.perf_counter()
    cache = Path(cache or Path.home() / ".cache" / "sportsleader-data")
    raw = cache / "download"
    out = cache / "export"
    raw.mkdir(parents=True, exist_ok=True)
    out.mkdir(parents=True, exist_ok=True)
    manifest_path = raw / "manifest.json"
    download(f"{RELEASE}/manifest/manifest.json", manifest_path)
    manifest = json.loads(manifest_path.read_text())
    latest, wanted = current_files(manifest)
    log(f"current seasons {latest}; {len(wanted)} current-season files")
    local = {}
    for entry in wanted:
        dest = raw / entry["league"] / entry["dataset"] / f"{entry['season']}.parquet"
        download(gh_publish.asset_url(entry["league"], entry["dataset"], entry["season"]), dest)
        local[entry["path"]] = dest
    log(f"download {time.perf_counter() - t0:.1f}s")

    db_path = cache / "state.db"
    for extra in cache.glob("state.db*"):
        extra.unlink()
    conn = db.connect(db_path)
    db.init_db(conn)
    teams = Teams(conn)
    loaded = {}
    # Games first so team ids exist for the stat tables.
    ordered = sorted(wanted, key=lambda e: 0 if e["dataset"] == "games" else 1)
    for entry in ordered:
        n = load_file(conn, teams, entry, local[entry["path"]])
        loaded[entry["path"]] = n
    snapshots.seed_lookups(conn)
    seen = snapshots.remember(conn)
    log(f"loaded {sum(loaded.values())} rows in {time.perf_counter() - t0:.1f}s")

    today = datetime.now(export.CENTRAL).date()
    dates = [today - timedelta(days=2), today - timedelta(days=1), today]
    log(f"ingest {dates[0]} .. {dates[-1]}")
    stats = backfill.run(conn, list(LEAGUES), 0, rate=0.0, today=today, only_dates=dates)
    log(f"scoreboards+summaries {stats} at {time.perf_counter() - t0:.1f}s")
    conn.commit()
    conn.isolation_level = None
    player_stats = players.extract(conn, list(LEAGUES), log=log)
    box_stats = teambox.extract(conn, list(LEAGUES), log=log)
    lab_stats = labstats.extract(conn, list(LEAGUES), log=log)
    snap = snapshots.refresh(conn, log=log)
    snapshots.restore(conn, seen)
    conn.close()
    log(f"extract+snapshots done at {time.perf_counter() - t0:.1f}s ({snap.get('requests')} snapshot requests)")

    for child in out.glob("*"):
        if child.is_file():
            child.unlink()
    exported = export.run(str(db_path), out, leagues=LEAGUES,
                          datasets=set(UPDATE) | set(SNAPSHOTS), log=log)
    by_path = {entry["path"]: entry for entry in manifest["files"]}
    report = []
    changed = []
    for entry in exported["files"]:
        rel = entry["path"]
        new_path = out / rel
        old_entry = by_path.get(rel)
        if old_entry is None:
            diff = {"rows_added": entry["rows"], "rows_updated": 0, "rows_removed": 0,
                    "column_mismatch": False}
        else:
            old_cols = [c["name"] for c in old_entry["columns"]]
            new_cols = [c["name"] for c in entry["columns"]]
            if old_cols != new_cols:
                raise RuntimeError(f"{rel} columns changed: {new_cols} != {old_cols}")
            diff = row_diff(local[rel], new_path)
            if diff["column_mismatch"]:
                raise RuntimeError(f"{rel} exported columns do not match the file")
        report.append((rel, diff))
        if diff["rows_added"] or diff["rows_updated"] or diff["rows_removed"]:
            fresh = file_entry(old_entry or entry, new_path, rel, diff)
            by_path[rel] = fresh
            changed.append((rel, new_path, fresh))
        log(f"  {rel}: +{diff['rows_added']} ~{diff['rows_updated']} -{diff['rows_removed']}")

    merged = with_urls({**manifest, "files": sorted(by_path.values(), key=lambda f: f["path"])})
    man_bytes = (json.dumps(merged, indent=1) + "\n").encode()
    total = time.perf_counter() - t0
    log(f"TOTAL {total:.1f}s; {len(changed)} files would upload; dry_run={dry_run}")
    log("not re-fetched: " + "; ".join(snapshots.NOT_REFRESHED))
    if dry_run or not changed:
        return {"seconds": round(total, 1), "changed": [c[0] for c in changed],
                "report": report, "backfill": stats,
                "players": player_stats, "box": box_stats, "stats": lab_stats}
    if not os.environ.get(gh_publish.TOKEN_ENV):
        raise RuntimeError(f"refusing to upload without {gh_publish.TOKEN_ENV}")
    for rel, path, fresh in changed:
        gh_publish.replace_asset(
            f"{fresh['league']}_{fresh['dataset']}", f"{fresh['season']}.parquet",
            path.read_bytes(), f"sha256:{fresh['sha256']}")
        log(f"uploaded {rel}")
    gh_publish.replace_asset("manifest", "manifest.json", man_bytes,
                             f"sha256:{hashlib.sha256(man_bytes).hexdigest()}")
    log("uploaded manifest")
    return {"seconds": round(total, 1), "changed": [c[0] for c in changed], "uploaded": True}


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--dry-run", action="store_true", help="fetch and export, upload nothing")
    ap.add_argument("--cache", default="")
    args = ap.parse_args(argv)
    try:
        result = run(dry_run=args.dry_run, cache=args.cache or None)
    except Exception as e:
        log(f"FAIL {type(e).__name__}: {e}")
        return 1
    log(json.dumps({k: result[k] for k in ("seconds", "changed")}, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
