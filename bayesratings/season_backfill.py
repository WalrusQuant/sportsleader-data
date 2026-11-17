"""Publish one league and one season.

    python -m bayesratings.season_backfill --league cfb --season 2024 --cache /tmp/cfb-2024

Started by hand (the backfill workflow). It is not part of the nightly run.
The database under --cache is reused, so a failed run continues at the first
unfinished date. --force deletes that database and uploads again even when
the season is already in the manifest.

Parquet files are uploaded first. The manifest is then re-read and only this
season's files are merged in, so a nightly publish in the same minute is kept.
"""

import argparse
import os
import shutil
import sys
from datetime import date
from pathlib import Path

from . import backfill, db, export, gh_publish, labstats, players, teambox

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


def run(league, season, cache, force=False, summary_workers=backfill.SUMMARY_WORKERS,
        today=None, fetch_manifest=None):
    today = today or date.today()
    league = league.strip().lower()
    season = check_season(league, season, today)
    cache = Path(cache)
    cache.mkdir(parents=True, exist_ok=True)
    fetch = fetch_manifest or gh_publish.fetch_manifest
    manifest = fetch()
    missing = missing_datasets(manifest, league, season)
    if not missing and not force:
        log(f"skip {league} {season}: already published")
        return {"skipped": True, "league": league, "season": season}
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
    stats = backfill.run(
        conn, [league], 0, rate=0.0, today=today, only_dates=dates,
        only_season=season, summary_workers=summary_workers)
    conn.commit()
    conn.isolation_level = None
    players.extract(conn, [league], log=log)
    teambox.extract(conn, [league], log=log)
    labstats.extract(conn, [league], log=log)
    conn.close()
    log(f"fetched {stats}")

    out = cache / "export"
    if out.exists():
        shutil.rmtree(out)
    wanted = set(expected_datasets(league))
    exported = export.run(str(db_path), out, leagues=(league,), datasets=wanted, log=log)
    files = [entry for entry in exported["files"]
             if entry["league"] == league and int(entry["season"]) == season]
    if not files:
        raise RuntimeError(f"no rows exported for {league} {season}")

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
    gh_publish.commit_manifest(
        files, export.dataset_catalog, fetch=fetch, upload=gh_publish.upload_manifest)
    log("uploaded manifest")
    present = {entry["dataset"] for entry in files}
    still_missing = [name for name in expected_datasets(league) if name not in present]
    if still_missing:
        log(f"warning: {league} {season} has no rows for {still_missing}")
    return {"skipped": False, "league": league, "season": season,
            "files": [entry["path"] for entry in files], "backfill": stats,
            "missing": still_missing}


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--league", required=True)
    ap.add_argument("--season", required=True, type=int)
    ap.add_argument("--cache", required=True)
    ap.add_argument("--force", action="store_true",
                    help="rebuild and upload even when the season is already published")
    ap.add_argument("--summary-workers", type=int, default=backfill.SUMMARY_WORKERS)
    args = ap.parse_args(argv)
    try:
        result = run(args.league, args.season, args.cache, force=args.force,
                     summary_workers=args.summary_workers)
    except Exception as exc:
        log(f"FAIL {backfill.public_error(exc)}")
        return 1
    log(f"done skipped={result['skipped']} league={result['league']} season={result['season']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
