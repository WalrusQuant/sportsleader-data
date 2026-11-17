"""Older pro seasons: box parsing, untracked zeros, and franchise names.

Fixtures are trimmed ESPN boxes. Nothing in this file calls the network.
"""

import json
import tempfile
import unittest
import urllib.error
from datetime import date, datetime
from pathlib import Path
from unittest import mock
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq

from bayesratings import backfill, coverage, db, espn, export, gh_publish, players
from bayesratings import queue_seasons, season_backfill

FIXTURES = Path(__file__).resolve().parent / "fixtures"
ROOT = Path(__file__).resolve().parent.parent
TODAY = date(2026, 11, 17)
CENTRAL = ZoneInfo("America/Chicago")


def _load(league, name):
    return json.loads((FIXTURES / league / name).read_text())


def _game(league, eid, season, home, away, day):
    return {
        "league": league, "espn_event_id": eid, "season": season, "season_type": 2,
        "game_date": day, "home_team_id": home, "away_team_id": away,
    }


class CutoffTest(unittest.TestCase):
    def test_check_season_accepts_the_verified_floors(self):
        self.assertEqual(season_backfill.check_season("nfl", 1999, TODAY), 1999)
        self.assertEqual(season_backfill.check_season("nba", 1994, TODAY), 1994)
        self.assertEqual(season_backfill.check_season("nhl", 1994, TODAY), 1994)
        self.assertEqual(season_backfill.check_season("mlb", 2003, TODAY), 2003)
        self.assertEqual(season_backfill.check_season("nfl", 2026, TODAY), 2026)
        for league, too_old in (("nfl", 1998), ("nba", 1993), ("nhl", 1993), ("mlb", 2002)):
            with self.assertRaises(ValueError):
                season_backfill.check_season(league, too_old, TODAY)

    def test_cancelled_nhl_season_does_not_fetch(self):
        def fetch():
            raise AssertionError("manifest was fetched")
        with tempfile.TemporaryDirectory() as tmp:
            result = season_backfill.run(
                "nhl", 2005, tmp, today=TODAY, fetch_manifest=fetch)
        self.assertTrue(result["skipped"])
        self.assertFalse(result["incomplete"])
        self.assertEqual(result["reason"], "not_played")


class OldBoxTest(unittest.TestCase):
    def test_nfl_1999_pads_a_short_passing_line_and_keeps_targets_at_zero(self):
        summary = _load("nfl", "190912001_summary.json")
        rows, dropped = players.parse_game(
            summary, _game("nfl", 190912001, 1999, 1, 16, "1999-09-12"),
            {1: 1, 16: 16})
        self.assertEqual(dropped, 0)
        passing = {r["athlete_id"]: r for r in rows["player_game_nfl_passing"]}
        cunningham = passing[1]
        self.assertEqual(cunningham["cmp"], 22)
        self.assertEqual(cunningham["att"], 33)
        self.assertEqual(cunningham["yds"], 184)
        self.assertEqual(cunningham["sacks"], 0)
        self.assertEqual(cunningham["sack_yds"], 0)
        self.assertAlmostEqual(cunningham["qbr"], 91.0)
        self.assertIsNone(cunningham["rating"])
        chandler = passing[4]
        self.assertEqual(chandler["sacks"], 2)
        self.assertEqual(chandler["sack_yds"], 12)
        self.assertIsNone(chandler["rating"])
        receiving = rows["player_game_nfl_receiving"]
        self.assertTrue(all(r["tgt"] == 0 for r in receiving))
        self.assertTrue(any(r["rec"] for r in receiving))
        blanked_rows, blanked = coverage.blank_untracked(receiving, ["tgt", "rec", "yds"])
        self.assertEqual(blanked, ["tgt"])
        self.assertTrue(all(r["tgt"] is None for r in blanked_rows))
        self.assertEqual(blanked_rows[0]["rec"], receiving[0]["rec"])

    def test_nba_1999_box_parses_with_a_zero_plus_minus(self):
        summary = _load("nba", "191102007_summary.json")
        rows, dropped = players.parse_game(
            summary, _game("nba", 191102007, 2000, 21, 7, "1999-11-02"),
            {21: 21, 7: 7})
        self.assertEqual(dropped, 0)
        logs = {r["athlete_id"]: r for r in rows["player_game_nba"]}
        self.assertEqual(logs[287]["pts"], 22)
        self.assertEqual(logs[287]["plus_minus"], 0)
        self.assertEqual(logs[287]["minutes"], 47)
        _, blanked = coverage.blank_untracked(rows["player_game_nba"], ["plus_minus", "pts"])
        self.assertEqual(blanked, ["plus_minus"])

    def test_a_real_plus_minus_keeps_the_column(self):
        rows = [{"plus_minus": 0}, {"plus_minus": -1}]
        _, blanked = coverage.blank_untracked(rows, ["plus_minus"])
        self.assertEqual(blanked, [])

    def test_nhl_1993_skater_ice_time_is_zero_and_goalie_ice_time_is_not(self):
        summary = _load("nhl", "131005003_summary.json")
        rows, dropped = players.parse_game(
            summary, _game("nhl", 131005003, 1994, 12, 3, "1993-10-05"),
            {12: 12, 3: 3})
        self.assertEqual(dropped, 0)
        skaters = rows["player_game_nhl_skater"]
        self.assertTrue(all(r["toi_sec"] == 0 for r in skaters))
        self.assertTrue(any(r["shots"] for r in skaters))
        self.assertTrue(any(r["pim"] for r in skaters))
        self.assertTrue(any(r["plus_minus"] == -1 for r in skaters))
        goalies = {r["athlete_id"]: r for r in rows["player_game_nhl_goalie"]}
        self.assertEqual(goalies[4350]["toi_sec"], 59 * 60 + 7)
        self.assertGreater(goalies[501]["toi_sec"], 0)
        _, skater_blanked = coverage.blank_untracked(skaters, ["toi_sec", "shots", "pim"])
        self.assertEqual(skater_blanked, ["toi_sec"])
        _, goalie_blanked = coverage.blank_untracked(
            rows["player_game_nhl_goalie"], ["toi_sec", "sv"])
        self.assertNotIn("toi_sec", goalie_blanked)

    def test_mlb_2003_batter_pitch_counts_are_zero_and_pitcher_counts_are_not(self):
        summary = _load("mlb", "230331106_summary.json")
        rows, dropped = players.parse_game(
            summary, _game("mlb", 230331106, 2003, 9, 6, "2003-03-31"),
            {9: 9, 6: 6})
        self.assertEqual(dropped, 0)
        batting = rows["player_game_mlb_batting"]
        self.assertTrue(all(r["pitches"] == 0 for r in batting))
        self.assertTrue(any(r["h"] for r in batting))
        pitching = {r["athlete_id"]: r for r in rows["player_game_mlb_pitching"]}
        self.assertEqual(pitching[3177]["pitches"], 78)
        self.assertEqual(pitching[3177]["outs"], 20)
        _, blanked = coverage.blank_untracked(batting, ["pitches", "h"])
        self.assertEqual(blanked, ["pitches"])

    def test_a_longer_stat_line_is_still_rejected(self):
        with self.assertRaises(players.BoxError):
            players.flatten(["a", "b"], ["1", "2", "3"])


class ZeroToNullExportTest(unittest.TestCase):
    def test_old_season_targets_become_null_and_columns_stay_put(self):
        spec = next(ds for ds in export.DATASETS if ds.name == "receiving_game_logs")
        expected = [name for name, _ in spec.columns("nfl")]
        with tempfile.TemporaryDirectory() as tmp:
            conn = db.connect(Path(tmp) / "t.db")
            db.init_db(conn)
            conn.execute(
                "INSERT INTO teams (team_id, league, espn_id, abbreviation, name, in_directory) "
                "VALUES (25, 'nba', 25, 'OKC', 'Oklahoma City Thunder', 1), "
                "(10, 'nfl', 10, 'TEN', 'Tennessee Titans', 1), "
                "(16, 'nfl', 16, 'MIN', 'Minnesota Vikings', 1)")
            conn.execute(
                "INSERT INTO team_seasons (team_id, league, season, name, abbreviation, home_games) "
                "VALUES (10, 'nfl', 2008, 'Tennessee Titans', 'TEN', 8), "
                "(16, 'nfl', 2008, 'Minnesota Vikings', 'MIN', 8)")
            conn.executemany(
                "INSERT INTO player_game_nfl_receiving (espn_event_id, season, season_type, "
                "game_date, team_id, opp_team_id, is_home, athlete_id, rec, tgt, yds, td, long, "
                "stats_json) VALUES (?, ?, 2, ?, ?, ?, 1, ?, ?, 0, ?, ?, 10, '{}')",
                [
                    (1, 2008, "2008-09-07", 16, 10, 3, 7, 70, 1),
                    (1, 2008, "2008-09-07", 10, 16, 4, 5, 40, 0),
                    (2, 2024, "2024-09-08", 16, 10, 3, 6, 80, 0),
                ])
            conn.commit()
            conn.close()
            out = Path(tmp) / "out"
            exported = export.run(str(Path(tmp) / "t.db"), out, leagues=("nfl",),
                                   datasets={"receiving_game_logs"}, log=lambda *_: None)
            old = pq.read_table(out / "nfl" / "receiving_game_logs" / "2008.parquet")
            current = pq.read_table(out / "nfl" / "receiving_game_logs" / "2024.parquet")
            self.assertEqual(old.column_names, expected)
            self.assertEqual(current.column_names, expected)
            self.assertEqual(old.column("targets").null_count, old.num_rows)
            self.assertEqual(old.column("receptions").null_count, 0)
            self.assertEqual(current.column("targets").null_count, 0)
            self.assertEqual(current.column("targets")[0].as_py(), 0)
            self.assertEqual(exported["history_blanked"]["nfl/2008"]["receiving_game_logs"],
                             ["targets"])
            self.assertNotIn("nfl/2024", exported["history_blanked"])
            conn.close()


class FranchiseTest(unittest.TestCase):
    def _games(self, conn, pairs, league="nba", season=1996, day="1995-11-05", start=1):
        conn.executemany(
            "INSERT INTO games (espn_event_id, league, season, season_type, "
            "start_time_utc, game_date, home_team_id, away_team_id, completed, status) "
            "VALUES (?, ?, ?, 2, ?, ?, ?, ?, 1, 'STATUS_FINAL')",
            [(start + i, league, season, f"{day}T18:00:00+00:00", day, home, away)
             for i, (home, away) in enumerate(pairs)])

    def test_directory_and_full_season_teams_stay(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn = db.connect(Path(tmp) / "t.db")
            db.init_db(conn)
            conn.execute(
                "INSERT INTO teams (team_id, league, espn_id, abbreviation, name, in_directory) "
                "VALUES (25, 'nba', 25, 'OKC', 'Oklahoma City Thunder', 1), "
                "(24, 'nhl', 24, 'ARI', 'Arizona Coyotes', 0), "
                "(99, 'nba', 99, 'EAST', 'East', 0), "
                "(100, 'nhl', 100, 'WST', 'West', 0), "
                "(101, 'nhl', 101, 'ASN', 'All-Stars', 0)")
            self._games(conn, [(25, 99)])
            self._games(conn, [(24, 100)] * 6 + [(24, 101)], league="nhl",
                        season=1996, day="1995-11-05", start=100)
            conn.commit()
            self.assertNotIn(25, db.exhibition_team_ids(conn, "nba"))
            self.assertIn(99, db.exhibition_team_ids(conn, "nba"))
            self.assertNotIn(24, db.exhibition_team_ids(conn, "nhl"))
            self.assertNotIn(100, db.exhibition_team_ids(conn, "nhl"))
            self.assertIn(101, db.exhibition_team_ids(conn, "nhl"))
            self.assertEqual(db.exhibition_team_ids(conn, "cfb"), set())
            conn.close()

    def test_scoreboard_name_survives_the_current_directory_name(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn = db.connect(Path(tmp) / "t.db")
            db.init_db(conn)
            conn.execute(
                "INSERT INTO teams (team_id, league, espn_id, abbreviation, name, in_directory) "
                "VALUES (25, 'nba', 25, 'OKC', 'Oklahoma City Thunder', 1), "
                "(29, 'nba', 29, 'MEM', 'Memphis Grizzlies', 1)")
            teams = [
                {"espn_id": 25, "name": "Seattle SuperSonics", "abbreviation": "SEA"},
                {"espn_id": 29, "name": "Vancouver Grizzlies", "abbreviation": "VAN"},
            ]
            db._remember_season_names(
                conn, "nba",
                [{"season": 1996, "home_espn_id": 25, "away_espn_id": 29}],
                {25: 25, 29: 29}, teams)
            conn.execute(
                "INSERT INTO games (espn_event_id, league, season, season_type, "
                "start_time_utc, game_date, home_team_id, away_team_id, completed, "
                "status, venue, neutral_site) VALUES "
                "(1, 'nba', 1996, 2, '1995-11-05T18:00:00+00:00', '1995-11-05', "
                "25, 29, 1, 'STATUS_FINAL', 'KeyArena', 0)")
            conn.execute(
                "INSERT INTO player_game_nba (espn_event_id, season, season_type, game_date, "
                "team_id, opp_team_id, is_home, athlete_id, starter, did_not_play, "
                "minutes, pts, plus_minus, stats_json) VALUES "
                "(1, 1996, 2, '1995-11-05', 25, 29, 1, 7, 1, 0, 36, 18, 0, '{}')")
            conn.commit()
            backfill.refresh_team_seasons(conn, "nba")
            row = conn.execute(
                "SELECT abbreviation, name, home_venue FROM team_seasons "
                "WHERE team_id = 25 AND season = 1996").fetchone()
            self.assertEqual(tuple(row), ("SEA", "Seattle SuperSonics", "KeyArena"))
            directory = conn.execute(
                "SELECT abbreviation, name FROM teams WHERE team_id = 25").fetchone()
            self.assertEqual(tuple(directory), ("OKC", "Oklahoma City Thunder"))
            conn.close()
            out = Path(tmp) / "out"
            export.run(str(Path(tmp) / "t.db"), out, leagues=("nba",),
                       datasets={"player_game_logs", "games"}, log=lambda *_: None)
            table = pq.read_table(out / "nba" / "player_game_logs" / "1996.parquet")
            games = pq.read_table(out / "nba" / "games" / "1996.parquet")
            self.assertEqual(games.column("home_team_abbr")[0].as_py(), "SEA")
            self.assertEqual(games.column("away_team_abbr")[0].as_py(), "VAN")
            self.assertEqual(table.column("team_abbr")[0].as_py(), "SEA")
            self.assertEqual(table.column("team_name")[0].as_py(), "Seattle SuperSonics")
            self.assertEqual(table.column("opponent_abbr")[0].as_py(), "VAN")
            self.assertEqual(table.column("opponent_name")[0].as_py(), "Vancouver Grizzlies")
            spec = next(ds for ds in export.DATASETS if ds.name == "player_game_logs")
            self.assertEqual(table.column_names, [name for name, _ in spec.columns("nba")])


class ScoreboardCleanupTest(unittest.TestCase):
    def test_padded_labels_collapse(self):
        self.assertEqual(espn.clean_label("New York        Islanders"), "New York Islanders")
        shown = espn._team_display({"id": "8", "abbreviation": "LA ", "displayName": "Los Angeles   Kings"})
        self.assertEqual(shown["abbreviation"], "LA")
        self.assertEqual(shown["name"], "Los Angeles Kings")
        self.assertIsNone(espn.clean_label(None))

    def test_shadow_duplicate_drops_only_the_empty_modern_twin(self):
        legacy = {"espn_event_id": 200115004, "season": 2000, "game_date": "2000-01-15",
                  "home_espn_id": 4, "away_espn_id": 6, "start_time_utc": "2000-01-16T00:30Z"}
        shadow = {"espn_event_id": 400218123, "season": 2000, "game_date": "2000-01-15",
                  "home_espn_id": 4, "away_espn_id": 6, "start_time_utc": "2000-01-15T17:00Z"}
        other_season = dict(shadow, espn_event_id=400216711, season=1999)
        modern_only = {"espn_event_id": 401234567, "season": 2024, "game_date": "2024-01-15",
                       "home_espn_id": 4, "away_espn_id": 6, "start_time_utc": "2024-01-15T17:00Z"}
        twin = dict(legacy, espn_event_id=200115005, start_time_utc="2000-01-16T04:00Z")
        kept = espn.drop_shadow_duplicates([legacy, shadow, other_season, modern_only, twin])
        ids = {g["espn_event_id"] for g in kept}
        self.assertNotIn(400218123, ids)
        self.assertEqual(ids, {200115004, 200115005, 400216711, 401234567})


class CoverageReportTest(unittest.TestCase):
    def test_a_thin_player_box_is_flagged(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn = db.connect(Path(tmp) / "t.db")
            db.init_db(conn)
            conn.execute(
                "INSERT INTO teams (team_id, league, espn_id, abbreviation, in_directory) "
                "VALUES (1, 'nba', 25, 'SEA', 1), (2, 'nba', 29, 'VAN', 1)")
            conn.executemany(
                "INSERT INTO games (espn_event_id, league, season, season_type, "
                "start_time_utc, game_date, home_team_id, away_team_id, completed, status) "
                "VALUES (?, 'nba', 1996, 2, '1995-11-05T18:00:00+00:00', '1995-11-05', 1, 2, 1, "
                "'STATUS_FINAL')", [(i,) for i in range(1, 21)])
            conn.execute(
                "INSERT INTO player_extract_log (espn_event_id, league, summary_fetched_at, "
                "extracted_at, version, status, rows) VALUES "
                "(1, 'nba', 't', 't', 1, 'ok', 10)")
            conn.execute(
                "INSERT INTO team_seasons (team_id, league, season, name, abbreviation, home_games) "
                "VALUES (1, 'nba', 1996, 'Seattle SuperSonics', 'SEA', 10)")
            conn.commit()
            report = coverage.build_report(
                conn, "nba", 1996, {"player_game_logs": ["plus_minus"]})
            conn.close()
        self.assertEqual(report["games"], 20)
        self.assertEqual(report["games_with_player_box"], 1)
        self.assertTrue(report["flagged_below_95"])
        self.assertEqual(report["blanked_stats"]["player_game_logs"], ["plus_minus"])
        self.assertEqual(report["teams"][0]["name"], "Seattle SuperSonics")
        self.assertFalse(coverage.blanks_history("nba", 2022))
        self.assertTrue(coverage.blanks_history("nba", 1996))
        self.assertFalse(coverage.blanks_history("cfb", 2021))


class PublishMergeTest(unittest.TestCase):
    def test_merge_keeps_a_coverage_entry_this_run_did_not_write(self):
        remote = {"version": 2, "files": [
            {"path": "nba/games/2024.parquet", "league": "nba", "dataset": "games",
             "season": 2024, "rows": 1, "sha256": "a", "columns": []}],
                  "coverage": {"nba:1996": {"sha256": "old"}}}
        merged = gh_publish.merge_manifest(remote, [], export.dataset_catalog)
        self.assertEqual(merged["coverage"]["nba:1996"]["sha256"], "old")
        added = gh_publish.merge_manifest(
            remote, [], export.dataset_catalog,
            coverage={"nfl:1999": {"sha256": "new", "games": 259}})
        self.assertEqual(added["coverage"]["nba:1996"]["sha256"], "old")
        self.assertEqual(added["coverage"]["nfl:1999"]["games"], 259)
        self.assertNotEqual(
            gh_publish.manifest_fingerprint(remote),
            gh_publish.manifest_fingerprint(added))


class ResumeAndQueueTest(unittest.TestCase):
    def test_deadline_stops_before_the_next_date(self):
        self.assertTrue(backfill.past_deadline(0, now=1))
        self.assertFalse(backfill.past_deadline(None, now=1))
        pending = season_backfill.slice_dates(
            [date(1999, 9, 12), date(1999, 9, 13), date(1999, 9, 14)],
            {"1999-09-12"}, max_dates=1)
        self.assertEqual(pending, [date(1999, 9, 13)])
        with tempfile.TemporaryDirectory() as tmp:
            conn = db.connect(Path(tmp) / "t.db")
            db.init_db(conn)
            conn.execute(
                "INSERT INTO teams (team_id, league, espn_id, abbreviation, in_directory) "
                "VALUES (1, 'nfl', 1, 'ATL', 1)")
            conn.commit()
            stats = backfill.run(
                conn, ["nfl"], 0, rate=0, today=TODAY,
                only_dates=[date(1999, 9, 12)], only_season=1999, deadline=0)
            conn.close()
        self.assertTrue(stats["stopped_early"])
        self.assertEqual(stats["dates"], 0)
        self.assertEqual(stats["games"], 0)

    def test_workers_stay_at_four_and_an_incomplete_run_does_not_upload(self):
        seen = {}

        def fake_run(conn, leagues, n_seasons, **kwargs):
            seen.update(kwargs)
            return {"errors": 0, "stopped_early": True, "games": 0}

        def fetch():
            return {"files": []}

        with tempfile.TemporaryDirectory() as tmp, \
                mock.patch.object(season_backfill.backfill, "run", fake_run):
            result = season_backfill.run(
                "nfl", 1999, tmp, today=TODAY, fetch_manifest=fetch,
                summary_workers=8, max_dates=2, max_minutes=150)
        self.assertEqual(seen["summary_workers"], 4)
        self.assertEqual(len(seen["only_dates"]), 2)
        self.assertTrue(result["incomplete"])
        self.assertIsNotNone(seen["deadline"])

    def test_retry_error_has_no_address(self):
        def denied(*_args, **_kwargs):
            raise urllib.error.HTTPError(
                "https://example.invalid/box", 429, "too many", None, None)

        with mock.patch("bayesratings.espn.urllib.request.urlopen", denied), \
                mock.patch("bayesratings.espn.time.sleep"):
            with self.assertRaises(RuntimeError) as caught:
                espn._get("https://example.invalid/box", retries=1)
        self.assertNotIn("http", str(caught.exception))
        self.assertNotIn("example", str(caught.exception))

    def test_queue_commands_are_one_league_and_skip_the_cancelled_season(self):
        seasons = queue_seasons.season_list("nhl", 2021, 1994)
        self.assertEqual(seasons[0], 2021)
        self.assertEqual(seasons[-1], 1994)
        self.assertNotIn(2005, seasons)
        self.assertEqual(queue_seasons.season_list("nfl", 2020, 1999)[0], 2020)
        text = "\n".join(queue_seasons.format_commands("nfl", [1999]))
        self.assertIn("gh workflow run backfill.yml", text)
        self.assertIn("--repo WalrusQuant/sportsleader-data", text)
        self.assertIn("league=nfl", text)
        self.assertIn("season=1999", text)
        self.assertNotIn("nightly-data", text)
        self.assertNotIn("SPORTSLEADER_DISPATCH_TOKEN", text)
        self.assertNotIn("CRONJOB_ORG_API_KEY", text)
        noon = datetime(2026, 11, 17, 12, tzinfo=CENTRAL)
        night = datetime(2026, 11, 17, 3, tzinfo=CENTRAL)
        self.assertTrue(queue_seasons.in_dispatch_window(noon))
        self.assertFalse(queue_seasons.in_dispatch_window(night))
        self.assertTrue(queue_seasons.in_dispatch_window(
            datetime(2026, 11, 17, 0, 30, tzinfo=CENTRAL)))

    def test_workflow_keeps_the_job_limit_and_stays_off_the_nightly_group(self):
        text = (ROOT / ".github" / "workflows" / "backfill.yml").read_text()
        self.assertIn("timeout-minutes: 180", text)
        self.assertIn("--summary-workers 4", text)
        self.assertIn("--max-minutes 150", text)
        self.assertIn("season-backfill-", text)
        self.assertNotIn("nightly-data", text)
        blob = text
        for name in ("season_backfill.py", "queue_seasons.py", "coverage.py", "espn.py"):
            blob += (ROOT / "bayesratings" / name).read_text()
        self.assertNotIn("SPORTSLEADER_DISPATCH_TOKEN", blob)
        self.assertNotIn("CRONJOB_ORG_API_KEY", blob)


if __name__ == "__main__":
    unittest.main()
