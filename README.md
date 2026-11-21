# sportsleader-data

Parquet files of game, player, team, and roster data for MLB, NBA, NFL, NHL, college football, and men's college basketball. One file per league, data set, and season. Current seasons are rebuilt every night. Older seasons are added by a backfill that is started by hand.

Downloads are GitHub Release assets. A file keeps the same URL when a new copy is published.

## URLs

```
https://github.com/WalrusQuant/sportsleader-data/releases/download/<tag>/<file>.parquet
```

- Tag: `<league>_<dataset>` (for example `nba_games`, `mlb_batting_game_logs`)
- File: `<season>.parquet` (for example `2027.parquet`)

Sample:

https://github.com/WalrusQuant/sportsleader-data/releases/download/nba_games/2027.parquet

The file list is `manifest.json` on the `manifest` release. Each entry has `url`, `rows`, `columns`, `bytes`, `sha256`, `league`, `dataset`, and `season`.

https://github.com/WalrusQuant/sportsleader-data/releases/download/manifest/manifest.json

A client can read the manifest and download a file only when its `sha256` has changed.

## Coverage

Pro seasons go back to the first year ESPN's player box is reliable. College seasons start later. The nightly job refreshes only the current season. Older seasons are added by the backfill below.

- NFL from 1999. The season number is the year the season starts. 1998 has no games.
- NBA from the 1993-94 season. The season number is the year the season ends, so 1993-94 is `1994`. The 1992-93 season has no player box.
- NHL from the 1993-94 season (`1994`). The 1992-93 season has no games. There is no 2004-05 season (`2005`); it was not played.
- MLB from 2003. The season number is the calendar year. 2002 has games and team lines and no player box.
- College football (`cfb`) from 2021. The season number is the year the season starts.
- Men's college basketball (`cbb`) from the 2020-21 season (`2021`). The season number is the year the season ends.

On a pro season older than the original files (MLB and NFL before 2021, NBA and NHL before 2022), a final that ESPN reports as 0-0 with no team or player box is published with empty scores and status `final_no_data`. The game stays in the schedule. It is left out of the team box and team counting files. The coverage report counts these as `games_missing_score`. A current season is not changed. A stat that is missing or zero on every row is stored as null. That is how ESPN filled stats it did not track yet: NHL skater ice time in the 1990s, NBA plus/minus before 2008-09, NFL receiving targets before 2009, and MLB batter pitch counts through at least 2008. A zero next to any other value in that season stays zero. Current-season files are not rewritten by this rule.

Each backfilled season also publishes a coverage report at `https://github.com/WalrusQuant/sportsleader-data/releases/download/coverage/<league>_<season>.json`. The manifest's `coverage` object points at it (`nba:1994` and the rest). The report has the game count, how many of those games had a player box, whether that share is under 95 percent, the stats that were stored as null, and the team abbreviation and name ESPN used that season. Relocated clubs keep that season's name (Seattle SuperSonics, Montreal Expos, Atlanta Thrashers, Hartford Whalers, Quebec Nordiques, Vancouver Grizzlies, Houston Oilers). A team on the current ESPN directory is not dropped for appearing a handful of times, and a team that played at least five games in the season is kept even if the franchise is gone.

College football is FBS only (ESPN group 80), including games against FCS teams and bowls. Men's college basketball is Division I (ESPN group 50), including games against non-Division I teams and the postseason. A college season file is created by the backfill workflow below. The nightly update then refreshes that current season.

Ids in the files are ESPN ids.

- `game_id` is the ESPN event id.
- `team_id`, `opponent_id`, `home_team_id`, and `away_team_id` are ESPN team ids.
- `player_id` is the ESPN athlete id.

Team abbreviations and names, and player names, are on the rows, so a file can be used on its own. `season_type` is `regular`, `postseason`, `preseason`, or `nba_cup_final`. Files are Parquet with zstd compression. Timestamps are UTC.

Column names and types for each file are in the manifest. This page does not repeat them.

## Data sets

| Data set | Leagues | What it holds |
|---|---|---|
| `games` | mlb, nba, nfl, nhl, cfb, cbb | Schedule and results, one row per game (upcoming games have no score). |
| `team_box_scores` | mlb, nba, nfl, nhl, cfb, cbb | ESPN team box line, one row per team per game. College rows use the pro columns and leave a stat blank when ESPN does not send it. |
| `team_game_stats` | mlb, nba, nfl, nhl, cfb, cbb | Full team counting stats for and against, one row per team per game. College football follows the NFL columns. College basketball follows the NBA columns. |
| `player_game_logs` | nba, cbb | NBA and men's college basketball box line, one row per player per game. College rows leave plus/minus blank. |
| `skater_game_logs` | nhl | NHL skater box line, one row per player per game. |
| `goalie_game_logs` | nhl | NHL goalie box line, one row per goalie per game. |
| `batting_game_logs` | mlb | MLB batting line, one row per batter per game. |
| `pitching_game_logs` | mlb | MLB pitching line, one row per pitcher per game. |
| `passing_game_logs` | nfl, cfb | NFL and college football passing line, one row per passer per game. College rows have QBR. Sacks, sack yards, and passer rating are blank when ESPN does not send them. |
| `rushing_game_logs` | nfl, cfb | NFL and college football rushing line, one row per rusher per game. |
| `receiving_game_logs` | nfl, cfb | NFL and college football receiving line, one row per receiver per game. College rows leave targets blank. |
| `rosters` | mlb, nba, nfl, nhl | ESPN roster entries with the time span each was current (valid_to empty = still current). |
| `injuries` | mlb, nba, nfl, nhl | Injury entries: ESPN (all leagues), NBA official report, NFL practice report. |
| `transactions` | mlb, nba, nfl, nhl | Transactions: ESPN (all leagues) and MLB official. |
| `depth_charts` | nfl | NFL depth chart slots with the time span each was current. |
| `probables` | mlb, nhl | Probable starting pitchers and goalies. ESPN lists the pre-game goalie (`source` `espn`, status Expected or Confirmed). After an NHL game, the official starter is added (`source` `nhl_api`, status Confirmed). |
| `lineups` | mlb, nba, nhl | Announced lineups: MLB batting orders, NBA starting fives, NHL even-strength lines and defense pairs, ESPN pre-game. NHL rows with `source` `nhl_api` are post-game: `role` is `line_ev`, `slot` is `f1`…`f4` for a forward line and `d1`…`d3` for a defense pair, and `game_id` / `game_date` are filled. A unit is a forward trio or defense pair that shared at least two minutes at 5-on-5, in any period including overtime. Each team keeps the four longest forward lines and three longest defense pairs that do not share a player. Older rows may still have `source` `daily_faceoff` and an empty `game_id`. No columns were added. |
| `weather` | mlb, nfl | Game weather: ESPN conditions and National Weather Service hourly outlook for the venue. |

## Update schedule

Every day at about 4:00 AM America/Chicago, from GitHub Actions in this repo. The job loads the current-season Parquet files and refreshes the last two days of games, team box scores, team game stats, and player game logs. Summary requests run a few at a time, and only for games that are new or whose score changed. College football and men's college basketball join that run after their current-season files have been published. A backfill of an older college season does not by itself start the nightly college update. It also refreshes current ESPN rosters, new ESPN injury details, ESPN transactions, NFL depth charts, and ESPN probables and weather. NBA.com starting lineups are refreshed for the last eight days through the next day. NHL starters, forward lines, and defense pairs are read from the official NHL API for completed games in that same window that do not already have both the official starters and the lines. A game whose shift chart is empty is read again on a later night until 48 hours after the opening faceoff. A changed file is published again at the same URL. Older seasons are left in place.

The Freshness watchdog workflow (`.github/workflows/freshness-watchdog.yml`) is dispatched around 5:30 AM America/Chicago. Its `check-freshness` job reads `generated_at` on the published manifest and fails when that timestamp is not today's America/Chicago date, or when the manifest cannot be read.

NBA official injury reports, NFL practice reports, MLB Stats API transactions, and National Weather Service forecasts are not re-fetched. Rows already published from those sources stay in the files. If a lineup page is missing or the source blocks the fetch, that page is logged and skipped. The rest of the nightly publish still runs.

## Backfill one season

`.github/workflows/backfill.yml` builds one league and one season, then uploads that season's files and its coverage report. It does not run on a schedule and it is not in the nightly job's concurrency group. Summary requests run 4 at a time. A request that is rate limited or a server error is retried with backoff. The job limit is 180 minutes. The script stops after 150 minutes, before the next date, and prints `RESULT incomplete=1`. Run the same command again: finished dates stay in the cached database and the upload waits until the season window is done. Add `-f force=true` only when that season should be rebuilt and uploaded again.

Start a run between 9:00 AM and 1:00 AM America/Chicago so it is finished before the 4:15 AM CT nightly job.

One season:

```
gh workflow run backfill.yml --repo WalrusQuant/sportsleader-data -f league=cfb -f season=2024
gh workflow run backfill.yml --repo WalrusQuant/sportsleader-data -f league=cbb -f season=2025
gh workflow run backfill.yml --repo WalrusQuant/sportsleader-data -f league=nfl -f season=1999
gh workflow run backfill.yml --repo WalrusQuant/sportsleader-data -f league=nba -f season=1994
gh workflow run backfill.yml --repo WalrusQuant/sportsleader-data -f league=nhl -f season=1994
gh workflow run backfill.yml --repo WalrusQuant/sportsleader-data -f league=mlb -f season=2003
```

Watch that run, then repeat the next season only after it finishes:

```
gh run list --repo WalrusQuant/sportsleader-data --workflow backfill.yml --limit 1 --json databaseId,status,url,createdAt
gh run watch <run-id> --repo WalrusQuant/sportsleader-data --exit-status
```

A range for one league, newest missing season first and one run at a time. The commands below print the `gh` lines. Add `--run` to dispatch them and wait. NHL 2005 is skipped. `--run` will not start between 1:00 AM and 9:00 AM CT.

```
python -m bayesratings.queue_seasons --league nfl --from-season 2020 --to-season 1999 --print
python -m bayesratings.queue_seasons --league nba --from-season 2021 --to-season 1994 --print
python -m bayesratings.queue_seasons --league nhl --from-season 2021 --to-season 1994 --print
python -m bayesratings.queue_seasons --league mlb --from-season 2020 --to-season 2003 --print
```

College football seasons are 2021 through the current season. Men's college basketball seasons are 2021 (2020-21) through the current season. The current season has to be backfilled once before the nightly job will refresh it. A backfill of an older season does not change the nightly league list.

## License and attribution

Game, box score, roster, injury, transaction, lineup, depth-chart, and weather rows are collected from public sources, chiefly ESPN. Some data sets also include rows from the MLB Stats API, the NBA official injury report, nflverse, the official NHL API, or the National Weather Service, as named in the data set above. Older lineup and probable files may still contain Daily Faceoff rows from before that switch.

sportsleader-data is not affiliated with ESPN, those sources, or the leagues. These files are a convenience copy of that data. No other license is granted here; the underlying data remains under the terms of its source. Attribute game data to ESPN (and to the other source named on the data set) and these files to sportsleader-data: https://github.com/WalrusQuant/sportsleader-data

These files do not include ratings, forecasts or betting lines.
