# sportsleader-data

Parquet files of game, player, team, and roster data for MLB, NBA, NFL, and NHL. One file per league, data set, and season. Rebuilt every night.

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

ESPN data from 2021 on.

- MLB and NFL seasons start at 2021. The season number is the year the season starts.
- NBA and NHL start at the 2021-22 season. The season number is the year the season ends, so 2021-22 is `2022`.

Ids in the files are ESPN ids.

- `game_id` is the ESPN event id.
- `team_id`, `opponent_id`, `home_team_id`, and `away_team_id` are ESPN team ids.
- `player_id` is the ESPN athlete id.

Team abbreviations and names, and player names, are on the rows, so a file can be used on its own. `season_type` is `regular`, `postseason`, `preseason`, or `nba_cup_final`. Files are Parquet with zstd compression. Timestamps are UTC.

Column names and types for each file are in the manifest. This page does not repeat them.

## Data sets

| Data set | Leagues | What it holds |
|---|---|---|
| `games` | mlb, nba, nfl, nhl | Schedule and results, one row per game (upcoming games have no score). |
| `team_box_scores` | mlb, nba, nfl, nhl | ESPN team box line, one row per team per game. |
| `team_game_stats` | mlb, nba, nfl, nhl | Full team counting stats for and against, one row per team per game. |
| `player_game_logs` | nba | NBA box line, one row per player per game. |
| `skater_game_logs` | nhl | NHL skater box line, one row per player per game. |
| `goalie_game_logs` | nhl | NHL goalie box line, one row per goalie per game. |
| `batting_game_logs` | mlb | MLB batting line, one row per batter per game. |
| `pitching_game_logs` | mlb | MLB pitching line, one row per pitcher per game. |
| `passing_game_logs` | nfl | NFL passing line, one row per passer per game. |
| `rushing_game_logs` | nfl | NFL rushing line, one row per rusher per game. |
| `receiving_game_logs` | nfl | NFL receiving line, one row per receiver per game. |
| `rosters` | mlb, nba, nfl, nhl | ESPN roster entries with the time span each was current (valid_to empty = still current). |
| `injuries` | mlb, nba, nfl, nhl | Injury entries: ESPN (all leagues), NBA official report, NFL practice report. |
| `transactions` | mlb, nba, nfl, nhl | Transactions: ESPN (all leagues) and MLB official. |
| `depth_charts` | nfl | NFL depth chart slots with the time span each was current. |
| `probables` | mlb, nhl | Probable starting pitchers and goalies (ESPN, Daily Faceoff). |
| `lineups` | mlb, nba, nhl | Announced lineups: MLB batting orders, NBA starting fives, NHL lines, ESPN pre-game. |
| `weather` | mlb, nfl | Game weather: ESPN conditions and National Weather Service hourly outlook for the venue. |

## Update schedule

Every day at about 4:00 AM America/Chicago, from GitHub Actions in this repo. The job loads the current-season Parquet files and refreshes the last two days of games, team box scores, team game stats, and player game logs. A changed file is published again at the same URL. Older seasons are left in place.

Rosters, injuries, transactions, depth charts, probables, lineups, and weather are not refreshed by this job. Those releases stay as published.

## License and attribution

Game, box score, roster, injury, transaction, lineup, depth-chart, and weather rows are collected from public sources, chiefly ESPN. Some data sets also include rows from the MLB Stats API, the NBA official injury report, nflverse, Daily Faceoff, or the National Weather Service, as named in the data set above.

sportsleader-data is not affiliated with ESPN, those sources, or the leagues. These files are a convenience copy of that data. No other license is granted here; the underlying data remains under the terms of its source. Attribute game data to ESPN (and to the other source named on the data set) and these files to sportsleader-data: https://github.com/WalrusQuant/sportsleader-data

These files do not include ratings, forecasts or betting lines.
