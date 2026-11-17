"""Publish one league and one season.

    python -m bayesratings.season_backfill --league nfl --season 1999 --cache /tmp/nfl-1999

Started by hand (the backfill workflow). It is not part of the nightly run.
The database under --cache is reused, so a stopped run continues at the first
unfinished date. --force deletes that database and uploads again even when
the season is already in the manifest.

The run stops itself after --max-minutes (default 150) so the job can save
the cache inside the 180-minute limit, and prints INCOMPLETE when dates
remain. Run the same command again to continue. Nothing is uploaded until
every date in the season window is done.

Parquet files are uploaded first. A coverage report for that season is
uploaded next to them. The manifest is then re-read and only this season's
files and coverage entry are merged in, so a nightly publish in the same
minute is kept.
"""

import argparse
import hashlib
import json
import os
import shutil
import sys
import time
from datetime import date
from pathlib import Path

from . import backfill, coverage, db, export, gh_publish, labstats, players, teambox

# Datasets a full season must publish before a later run will skip it.
EXPECTED = {
    "nba": ["player_game_logs"],
    "cbb": ["player_game_logs"],
    "nfl": ["passing_game_logs", "rushing_game_logs", "receiving_game_logs"],
    "cfb": ["passing_game_logs", "rushing_game_logs", "receiving_game_logs"],
    "nhl": ["skater_game_logs", "goalie_game_logs"],
    "mlb": ["batting_game_logs", "pitching_game_logs"],
}
COMMON = ["games", "team_box_scores", "team_game_stats"]
# The 2004-05 NHL season was cancelled. The season number would have been 2005.
NOT_PLAYED = {("nhl", 2005)}
MAX_SUMMARY_WORKERS = 4


def log(msg):
    print(msg, flush=True)


def expected_datasets(league):
    if league not in EXPECTED:
        raise ValueError(f"unknown league {league!r}")
    return COMMON + EXPECTED[league]


def published_datasets(manifest, league, season):
    have = set()
    for entry in (manifest or {}).get("files") or []:
        if entry.get("league") != league:
            continue
        try:
            if int(entry.get("season")) != int(season):
                continue
        except (TypeError, ValueError):
            continue
        have.add(entry.get("dataset"))
    return have


def missing_datasets(manifest, league, season):
    have = published_datasets(manifest, league, season)
    return [name for name in expected_datasets(league) if name not in have]


def check_season(league, season, today):
    """Reject a season this repo does not publish, or one that has not started."""
    if league not in backfill.FIRST_SEASON:
        raise ValueError(f"unknown league {league!r}")
    season = int(season)
    first = backfill.FIRST_SEASON[league]
    current = backfill.current_season(league, today)
    if season < first or season > current:
        raise ValueError(f"{league} season {season} is outside {first}..{current}")
    return season


def finished_dates(conn, league):
    rows = conn.execute(
        "SELECT date FROM backfill_dates WHERE league = ? AND status = 'done'",
        (league,))
    return {r[0] for r in rows}


def slice_dates(dates, done, max_dates=None):
    """Unfinished dates, optionally only the next chunk."""
    pending = [d for d in dates if d.isoformat() not in done]
    if max_dates:
        return pending[:int(max_dates)]
    return pending


def _result_line(result):
    incomplete = int(bool(result.get("incomplete")))
    log(f"RESULT incomplete={incomplete} skipped={result.get('skipped')} "
        f"league={result.get('league')} season={result.get('season')} "
        f"games={result.get('games', '')}")


def run(league, season, cache, force=False, summary_workers=backfill.SUMMARY_WORKERS,
        today=None, fetch_manifest=None, max_minutes=150, max_dates=None):
    today = today or date.today()
    league = league.strip().lower()
    season = check_season(league, season, today)
    workers = max(1, min(int(summary_workers or 1), MAX_SUMMARY_WORKERS))
    if (league, season) in NOT_PLAYED:
        log(f"skip {league} {season}: season was not played")
        result = {"skipped": True, "incomplete": False, "league": league,
                  "season": season, "reason": "not_played", "games": 0}
        _result_line(result)
        return result
    cache = Path(cache)
    cache.mkdir(parents=True, exist_ok=True)
    fetch = fetch_manifest or gh_publish.fetch_manifest
    manifest = fetch()
    missing = missing_datasets(manifest, league, season)
    if not missing and not force:
        log(f"skip {league} {season}: already published")
        result = {"skipped": True, "incomplete": False, "league": league, "season": season}
        _result_line(result)
        return result
    if missing:
        log(f"{league} {season} missing {missing}")
    if force:
        for extra in cache.glob("state.db*"):
            extra.unlink()
        log(f"force: cleared cached database for {league} {season}")
    db_path = cache / "state.db"
    conn = db.connect(db_path)
    db.init_db(conn)
    dates = backfill.season_dates(league, season, today)
    chunk = slice_dates(dates, finished_dates(conn, league), max_dates)
    deadline = None
    if max_minutes:
        deadline = time.monotonic() + float(max_minutes) * 60
    stats = backfill.run(
        conn, [league], 0, rate=0.0, today=today, only_dates=chunk,
        only_season=season, summary_workers=workers, deadline=deadline)
    conn.commit()
    remaining = [d for d in dates if d.isoformat() not in finished_dates(conn, league)]
    log(f"fetched {stats}")
    if remaining or stats.get("errors") or stats.get("stopped_early"):
        conn.close()
        result = {"skipped": False, "incomplete": True, "league": league,
                  "season": season, "backfill": stats, "remaining_dates": len(remaining),
                  "errors": stats.get("errors") or 0, "games": stats.get("games")}
        log(f"INCOMPLETE remaining_dates={len(remaining)} errors={result['errors']}")
        _result_line(result)
        return result
    played = conn.execute(
        "SELECT COUNT(*) FROM games WHERE league = ? AND season = ? "
        "AND season_type IN (2, 3) AND completed = 1",
        (league, season)).fetchone()[0]
    if not played:
        conn.close()
        log(f"{league} {season}: no completed games")
        result = {"skipped": True, "incomplete": False, "league": league,
                  "season": season, "reason": "no_games", "games": 0}
        _result_line(result)
        return result
    cleared = coverage.clear_scoreless_finals(conn, league, season)
    if cleared:
        log(f"{league} {season}: {cleared} final games had a 0-0 score and no box")
    conn.isolation_level = None
    players.extract(conn, [league], log=log)
    teambox.extract(conn, [league], log=log)
    labstats.extract(conn, [league], log=log)
    conn.close()

    out = cache / "export"
    if out.exists():
        shutil.rmtree(out)
    wanted = set(expected_datasets(league))
    exported = export.run(str(db_path), out, leagues=(league,), datasets=wanted, log=log)
    files = [entry for entry in exported["files"]
             if entry["league"] == league and int(entry["season"]) == season]
    if not files:
        raise RuntimeError(f"no rows exported for {league} {season}")
    blanked = (exported.get("history_blanked") or {}).get(f"{league}/{season}") or {}
    conn = db.connect(db_path)
    report = coverage.build_report(conn, league, season, blanked)
    conn.close()
    body = (json.dumps(report, indent=1) + "\n").encode()
    digest = hashlib.sha256(body).hexdigest()
    report_path = out / "coverage" / league / f"{season}.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_bytes(body)
    coverage_item = dict(report)
    coverage_item.update({
        "sha256": digest,
        "bytes": len(body),
        "url": gh_publish.coverage_url(league, season),
        "path": f"coverage/{league}/{season}.json",
    })

    remote = fetch()
    by_path = {entry["path"]: entry for entry in remote.get("files") or []}
    for entry in files:
        old = by_path.get(entry["path"])
        if not old:
            continue
        old_cols = [c["name"] for c in old.get("columns") or []]
        new_cols = [c["name"] for c in entry.get("columns") or []]
        if old_cols != new_cols:
            raise RuntimeError(f"{entry['path']} columns changed")
    if not os.environ.get(gh_publish.TOKEN_ENV):
        raise RuntimeError(f"refusing to upload without {gh_publish.TOKEN_ENV}")
    for entry in files:
        path = out / entry["path"]
        gh_publish.replace_asset(
            f"{league}_{entry['dataset']}", f"{season}.parquet",
            path.read_bytes(), f"sha256:{entry['sha256']}")
        log(f"uploaded {entry['path']}")
    gh_publish.replace_asset("coverage", f"{league}_{season}.json", body, f"sha256:{digest}")
    log(f"uploaded coverage/{league}/{season}.json")
    gh_publish.commit_manifest(
        files, export.dataset_catalog, fetch=fetch, upload=gh_publish.upload_manifest,
        coverage={f"{league}:{season}": coverage_item})
    log("uploaded manifest")
    if report["flagged_below_95"]:
        log(f"warning: {league} {season} player box coverage "
            f"{report['player_box_coverage']} is below 0.95")
    present = {entry["dataset"] for entry in files}
    still_missing = [name for name in expected_datasets(league) if name not in present]
    if still_missing:
        log(f"warning: {league} {season} has no rows for {still_missing}")
    result = {"skipped": False, "incomplete": False, "league": league, "season": season,
              "files": [entry["path"] for entry in files], "backfill": stats,
              "missing": still_missing, "games": played, "coverage": report,
              "blanked": blanked}
    _result_line(result)
    return result


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--league", required=True)
    ap.add_argument("--season", required=True, type=int)
    ap.add_argument("--cache", required=True)
    ap.add_argument("--force", action="store_true",
                    help="rebuild and upload even when the season is already published")
    ap.add_argument("--summary-workers", type=int, default=backfill.SUMMARY_WORKERS)
    ap.add_argument("--max-minutes", type=float, default=150,
                    help="stop before the next date after this many minutes (0 waits for the season)")
    ap.add_argument("--max-dates", type=int, default=0,
                    help="fetch at most this many unfinished dates, then stop")
    args = ap.parse_args(argv)
    try:
        result = run(args.league, args.season, args.cache, force=args.force,
                     summary_workers=args.summary_workers,
                     max_minutes=args.max_minutes,
                     max_dates=args.max_dates or None)
    except Exception as exc:
        log(f"FAIL {backfill.public_error(exc)}")
        return 1
    if result.get("incomplete") and result.get("errors"):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
