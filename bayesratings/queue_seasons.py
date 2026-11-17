"""Queue one league's seasons on the manual backfill workflow, one run at a time.

    python -m bayesratings.queue_seasons --league nfl --from-season 2020 --to-season 1999 --print

``--print`` writes the ``gh`` commands and does not dispatch them. ``--run``
dispatches each season, waits for that run to finish, and starts the next
only then. A run that stops early (``RESULT incomplete=1``) is dispatched
again, up to ``--attempts`` times. The workflow is not in the nightly
concurrency group.

``--run`` refuses to start between 1:00 AM and 9:00 AM America/Chicago, so a
150-minute run is done before the 4:15 AM CT nightly job. ``--anytime``
overrides that for a manual exception.

NHL 2005 is the cancelled 2004-05 season and is left out of every range.
"""

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from . import season_backfill

REPO = "WalrusQuant/sportsleader-data"
WORKFLOW = "backfill.yml"
CENTRAL = ZoneInfo("America/Chicago")
# Start at 9:00 or later, or before 1:00. A 150-minute run then ends before
# the 4:15 AM CT nightly job.
QUIET_HOURS = range(1, 9)


def season_list(league, start, end):
    """Seasons from `start` through `end`, inclusive, walking toward `end`.

    NHL 2005 is omitted. Passing 2020 then 1999 walks newest first, which
    publishes the seasons just before the current files before the oldest."""
    start, end = int(start), int(end)
    step = 1 if end >= start else -1
    out = []
    season = start
    while True:
        if (league, season) not in season_backfill.NOT_PLAYED:
            out.append(season)
        if season == end:
            break
        season += step
    return out


def in_dispatch_window(now) -> bool:
    return now.hour not in QUIET_HOURS


def dispatch_command(league, season, repo=REPO):
    return [
        "gh", "workflow", "run", WORKFLOW,
        "--repo", repo,
        "-f", f"league={league}",
        "-f", f"season={season}",
    ]


def watch_command(run_id, repo=REPO):
    return ["gh", "run", "watch", str(run_id), "--repo", repo, "--exit-status"]


def format_commands(league, seasons, repo=REPO):
    lines = []
    for season in seasons:
        lines.append(" ".join(dispatch_command(league, season, repo)))
        lines.append(
            f"gh run list --repo {repo} --workflow {WORKFLOW} --limit 1 "
            "--json databaseId,status,url,createdAt")
        lines.append(f"gh run watch <run-id> --repo {repo} --exit-status")
    return lines


def _run(argv):
    return subprocess.run(argv, check=False, text=True, capture_output=True)


def _newest_run(repo, started, runner=_run):
    listed = runner([
        "gh", "run", "list", "--repo", repo, "--workflow", WORKFLOW,
        "--limit", "5",
        "--json", "databaseId,createdAt,status,url,event",
    ])
    if listed.returncode != 0:
        raise RuntimeError(listed.stderr.strip() or "could not list workflow runs")
    rows = json.loads(listed.stdout or "[]")
    rows.sort(key=lambda row: row.get("createdAt") or "", reverse=True)
    for row in rows:
        created = row.get("createdAt") or ""
        if created >= started:
            return row
    return rows[0] if rows else None


def run_range(league, seasons, repo=REPO, attempts=6, sleep=time.sleep, runner=_run,
              now=None, ignore_window=False):
    """Dispatch seasons one at a time. Returns 0 when every season finishes."""
    clock = now or datetime.now(CENTRAL)
    if not ignore_window and not in_dispatch_window(clock):
        print(f"outside the 9:00 AM to 1:00 AM CT window ({clock:%H:%M} CT); not dispatching")
        return 2
    for season in seasons:
        done = False
        for attempt in range(1, attempts + 1):
            started = (datetime.now(timezone.utc) - timedelta(seconds=15)).strftime(
                "%Y-%m-%dT%H:%M:%SZ")
            print(f"dispatch {league} {season} attempt {attempt}", flush=True)
            dispatched = runner(dispatch_command(league, season, repo))
            if dispatched.returncode != 0:
                print(dispatched.stderr.strip() or "dispatch failed", flush=True)
                return 1
            run = None
            for _ in range(12):
                sleep(5)
                run = _newest_run(repo, started, runner)
                if run:
                    break
            if not run:
                print(f"no run appeared for {league} {season}", flush=True)
                return 1
            print(f"watching {run.get('databaseId')} {run.get('url') or ''}", flush=True)
            watched = runner(watch_command(run["databaseId"], repo))
            viewed = runner([
                "gh", "run", "view", str(run["databaseId"]), "--repo", repo, "--log",
            ])
            text = viewed.stdout or ""
            incomplete = "RESULT incomplete=1" in text
            if watched.returncode == 0 and not incomplete:
                done = True
                break
            print(f"{league} {season} not finished (exit {watched.returncode}, "
                  f"incomplete={int(incomplete)}); retrying", flush=True)
        if not done:
            print(f"{league} {season} still incomplete after {attempts} runs", flush=True)
            return 1
    return 0


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--league", required=True)
    ap.add_argument("--from-season", required=True, type=int)
    ap.add_argument("--to-season", required=True, type=int)
    ap.add_argument("--repo", default=REPO)
    ap.add_argument("--print", dest="show", action="store_true",
                    help="print the gh commands and do not dispatch")
    ap.add_argument("--run", action="store_true",
                    help="dispatch each season and wait for it")
    ap.add_argument("--anytime", action="store_true",
                    help="allow --run during the nightly quiet hours")
    ap.add_argument("--attempts", type=int, default=6)
    args = ap.parse_args(argv)
    league = args.league.strip().lower()
    if league not in ("mlb", "nba", "nfl", "nhl", "cfb", "cbb"):
        print(f"unknown league {league}", file=sys.stderr)
        return 1
    seasons = season_list(league, args.from_season, args.to_season)
    if args.show or not args.run:
        for line in format_commands(league, seasons, args.repo):
            print(line)
        if not args.run:
            return 0
    return run_range(league, seasons, args.repo, attempts=args.attempts,
                     ignore_window=args.anytime)


if __name__ == "__main__":
    sys.exit(main())
