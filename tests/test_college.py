"""College football and men's college basketball, offline.

Fixtures are trimmed ESPN boxes. Nothing in this file calls the network.
"""

import json
import tempfile
import threading
import time
import unittest
from datetime import date
from pathlib import Path

from bayesratings import backfill, db, export, gh_publish, labstats, players, teambox
from bayesratings import season_backfill
from bayesratings.nightly import leagues_to_update, log_table

FIXTURES = Path(__file__).resolve().parent / "fixtures"
ROOT = Path(__file__).resolve().parent.parent
TODAY = date(2026, 11, 17)


def _load(league, name):
    return json.loads((FIXTURES / league / name).read_text())


def _game(league, eid, season, home, away, day):
    return {
        "league": league, "espn_event_id": eid, "season": season, "season_type": 2,
        "game_date": day, "home_team_id": home, "away_team_id": away,
    }


class ScoreboardParamsTest(unittest.TestCase):
    def test_pro_query_is_unchanged(self):
        got = backfill.espn.scoreboard_params("nba", dates="20261117")
        self.assertEqual(got["limit"], 300)
        self.assertNotIn("groups", got)
        self.assertEqual(got["dates"], "20261117")

    def test_college_groups_and_limits(self):
        cfb = backfill.espn.scoreboard_params("cfb", dates="20241116")
        cbb = backfill.espn.scoreboard_params("cbb", dates="20261102")
        self.assertEqual(cfb["groups"], 80)
        self.assertEqual(cfb["limit"], 300)
        self.assertEqual(cbb["groups"], 50)
        self.assertGreaterEqual(cbb["limit"], 400)
        self.assertEqual(backfill.espn.scoreboard_params("cbb", limit=50)["limit"], 50)


class SeasonWindowTest(unittest.TestCase):
    def test_current_season_matches_pro_rules(self):
        self.assertEqual(backfill.current_season("nfl", TODAY), 2026)
        self.assertEqual(backfill.current_season("nba", date(2026, 9, 14)), 2026)
        self.assertEqual(backfill.current_season("nba", date(2026, 9, 15)), 2027)
        self.assertEqual(backfill.current_season("mlb", date(2026, 2, 1)), 2025)
        self.assertEqual(backfill.current_season("nhl", TODAY), 2027)

    def test_college_season_labels(self):
        self.assertEqual(backfill.current_season("cfb", TODAY), 2026)
        self.assertEqual(backfill.current_season("cfb", date(2026, 7, 1)), 2025)
        self.assertEqual(backfill.current_season("cbb", TODAY), 2027)
        self.assertEqual(backfill.current_season("cbb", date(2026, 4, 6)), 2026)
        self.assertEqual(backfill.current_season("cbb", date(2026, 10, 14)), 2026)
        self.assertEqual(backfill.current_season("cbb", date(2026, 10, 15)), 2027)
        start, end = backfill.season_window("cfb", 2024)
        self.assertEqual(start, date(2024, 8, 1))
        self.assertLess(date(2025, 1, 20), end)
        start, end = backfill.season_window("cbb", 2021)
        self.assertLessEqual(start, date(2020, 11, 25))
        self.assertGreaterEqual(end, date(2021, 4, 5))

    def test_check_season_bounds(self):
        self.assertEqual(season_backfill.check_season("cfb", 2021, TODAY), 2021)
        self.assertEqual(season_backfill.check_season("cbb", 2021, TODAY), 2021)
        self.assertEqual(season_backfill.check_season("cfb", 2026, TODAY), 2026)
        self.assertEqual(season_backfill.check_season("cbb", 2027, TODAY), 2027)
        with self.assertRaises(ValueError):
            season_backfill.check_season("cfb", 2020, TODAY)
        with self.assertRaises(ValueError):
            season_backfill.check_season("cbb", 2020, TODAY)
        with self.assertRaises(ValueError):
            season_backfill.check_season("cfb", 2027, TODAY)


class ParseFixtureTest(unittest.TestCase):
    def test_cfb_box_mirrors_nfl_and_leaves_missing_stats_blank(self):
        summary = _load("cfb", "401628551_summary.json")
        rows, dropped = players.parse_game(
            summary, _game("cfb", 401628551, 2024, 1, 2, "2024-11-16"),
            {275: 1, 2483: 2})
        self.assertEqual(dropped, 0)
        passing = {r["athlete_id"]: r for r in rows["player_game_cfb_passing"]}
        gabriel = passing[4427238]
        self.assertEqual(gabriel["cmp"], 22)
        self.assertEqual(gabriel["att"], 31)
        self.assertEqual(gabriel["yds"], 218)
        self.assertEqual(gabriel["td"], 0)
        self.assertEqual(gabriel["ints"], 1)
        self.assertAlmostEqual(gabriel["qbr"], 60.5)
        self.assertIsNone(gabriel["sacks"])
        self.assertIsNone(gabriel["sack_yds"])
        self.assertIsNone(gabriel["rating"])
        self.assertEqual(gabriel["qb_starter"], 1)
        self.assertEqual(passing[4702829]["cmp"], 12)
        self.assertEqual(passing[4702829]["qb_starter"], 1)
        self.assertTrue(rows["player_game_cfb_rushing"])
        self.assertTrue(rows["player_game_cfb_receiving"])
        self.assertTrue(all(r["tgt"] is None for r in rows["player_game_cfb_receiving"]))
        self.assertNotIn("player_game_nfl_passing", rows)

    def test_cbb_box_mirrors_nba_without_plus_minus(self):
        summary = _load("cbb", "401743776_summary.json")
        rows, _ = players.parse_game(
            summary, _game("cbb", 401743776, 2025, 1, 2, "2025-03-15"),
            {97: 1, 150: 2})
        lines = rows["player_game_cbb"]
        self.assertGreaterEqual(len(lines), 30)
        self.assertTrue(any(r["did_not_play"] == 1 for r in lines))
        played = [r for r in lines if not r["did_not_play"]]
        self.assertTrue(played)
        self.assertTrue(all(r["plus_minus"] is None for r in lines))
        self.assertTrue(any(r["pts"] and r["pts"] > 0 for r in played))
        self.assertTrue(any(r["position"] in ("G", "F", "C") for r in lines))
        self.assertNotIn("player_game_nba", rows)

    def test_team_counts(self):
        cfb = _load("cfb", "401628551_summary.json")
        packed = labstats.parse(cfb, "cfb", ["2483", "275"])
        names = labstats.COUNTS["cfb"]
        ore = dict(zip(names, packed["2483"]))
        self.assertEqual(ore["yd"], 354)
        self.assertEqual(ore["cmp"], 22)
        self.assertEqual(ore["att"], 31)
        self.assertEqual(ore["top"], 32 * 60 + 23)
        self.assertEqual(ore["d3m"], 5)
        self.assertEqual(ore["d3a"], 15)
        self.assertIsNone(ore["def_qbh"])
        self.assertGreater(ore["drives"], 0)
        wis = dict(zip(names, packed["275"]))
        self.assertEqual(wis["yd"], 226)

        cbb = _load("cbb", "401743776_summary.json")
        packed = labstats.parse(cbb, "cbb", ["97", "150"])
        lou = dict(zip(labstats.COUNTS["cbb"], packed["97"]))
        self.assertEqual(lou["fgm"], 25)
        self.assertEqual(lou["fga"], 64)
        self.assertEqual(lou["minutes"], 40)
        self.assertIsNotNone(lou["poss"])

    def test_nfl_qb_hits_stay_zero_when_the_box_sends_them(self):
        summary = {"boxscore": {
            "teams": [{"team": {"id": "1"}, "statistics": [
                {"name": "totalYards", "displayValue": "100"}]}],
            "players": [{"team": {"id": "1"}, "statistics": [{
                "name": "defensive",
                "keys": ["sacks", "tacklesForLoss", "passesDefended", "QBHits"],
                "athletes": [{"athlete": {"id": "9"}, "stats": ["0", "0", "0", "0"]}],
            }]}],
        }, "drives": {"previous": []}}
        packed = labstats.parse(summary, "nfl", ["1", "2"])
        nfl = dict(zip(labstats.COUNTS["nfl"], packed["1"]))
        self.assertEqual(nfl["def_qbh"], 0)
        college = dict(summary)
        college["boxscore"] = json.loads(json.dumps(summary["boxscore"]))
        college["boxscore"]["players"][0]["statistics"][0]["keys"] = [
            "sacks", "tacklesForLoss", "passesDefended", "hurries"]
        college["boxscore"]["players"][0]["statistics"][0]["athletes"][0]["stats"] = [
            "0", "0", "0", "1"]
        packed = labstats.parse(college, "cfb", ["1"])
        cfb = dict(zip(labstats.COUNTS["cfb"], packed["1"]))
        self.assertIsNone(cfb["def_qbh"])
        self.assertEqual(cfb["def_sk"], 0)


class SchemaMirrorTest(unittest.TestCase):
    def test_exported_columns_match_the_pro_league(self):
        by_name = export.BY_NAME
        for name in ("passing_game_logs", "rushing_game_logs", "receiving_game_logs"):
            self.assertEqual(by_name[name].columns("cfb"), by_name[name].columns("nfl"))
            self.assertIn("cfb", by_name[name].leagues)
            self.assertIn("nfl", by_name[name].leagues)
        self.assertEqual(by_name["player_game_logs"].columns("cbb"),
                         by_name["player_game_logs"].columns("nba"))
        self.assertEqual(by_name["team_box_scores"].columns("cfb"),
                         by_name["team_box_scores"].columns("nfl"))
        self.assertEqual(by_name["team_box_scores"].columns("cbb"),
                         by_name["team_box_scores"].columns("nba"))
        self.assertEqual(by_name["team_game_stats"].columns("cfb"),
                         by_name["team_game_stats"].columns("nfl"))
        self.assertEqual(by_name["team_game_stats"].columns("cbb"),
                         by_name["team_game_stats"].columns("nba"))
        self.assertEqual(by_name["games"].columns("cfb"), by_name["games"].columns("nfl"))

    def test_snapshot_datasets_stay_pro_only(self):
        for name in ("rosters", "injuries", "transactions", "depth_charts",
                     "probables", "lineups", "weather"):
            leagues = export.BY_NAME[name].leagues
            self.assertNotIn("cfb", leagues)
            self.assertNotIn("cbb", leagues)

    def test_log_tables_do_not_share_storage(self):
        self.assertEqual(log_table("nfl", "passing_game_logs"), "player_game_nfl_passing")
        self.assertEqual(log_table("cfb", "passing_game_logs"), "player_game_cfb_passing")
        self.assertEqual(log_table("nba", "player_game_logs"), "player_game_nba")
        self.assertEqual(log_table("cbb", "player_game_logs"), "player_game_cbb")

    def test_export_roundtrip_uses_separate_tables(self):
        summary = _load("cfb", "401628551_summary.json")
        with tempfile.TemporaryDirectory() as tmp:
            conn = db.connect(Path(tmp) / "t.db")
            db.init_db(conn)
            conn.execute(
                "INSERT INTO teams (team_id, league, espn_id, abbreviation, name) "
                "VALUES (1, 'cfb', 275, 'WIS', 'Wisconsin Badgers'), "
                "(2, 'cfb', 2483, 'ORE', 'Oregon Ducks')")
            rows, _ = players.parse_game(
                summary, _game("cfb", 401628551, 2024, 1, 2, "2024-11-16"),
                {275: 1, 2483: 2})
            cols = {t: [r[1] for r in conn.execute(f"PRAGMA table_info({t})")]
                    for t in players.TABLES["cfb"]}
            players.store_game(conn, "cfb", 401628551, rows, cols)
            players._upsert_names(conn, "cfb", rows)
            conn.commit()
            conn.close()
            out = Path(tmp) / "out"
            manifest = export.run(Path(tmp) / "t.db", out, leagues=("cfb",),
                                  datasets={"passing_game_logs"}, log=lambda _m: None)
            import pandas as pd
            frame = pd.read_parquet(out / "cfb" / "passing_game_logs" / "2024.parquet")
            self.assertEqual(list(frame.columns),
                             [c[0] for c in export.BY_NAME["passing_game_logs"].columns("nfl")])
            gabriel = frame[frame["player_id"] == 4427238].iloc[0]
            self.assertEqual(int(gabriel["completions"]), 22)
            self.assertEqual(int(gabriel["attempts"]), 31)
            self.assertTrue(pd.isna(gabriel["sacks"]))
            self.assertAlmostEqual(float(gabriel["qbr"]), 60.5)
            paths = [f["path"] for f in manifest["files"]]
            self.assertIn("cfb/passing_game_logs/2024.parquet", paths)
            self.assertTrue(all(not p.startswith("nfl/") for p in paths))


class SummarySelectionTest(unittest.TestCase):
    def _game(self, eid, completed=True, home=10, away=7):
        return {"espn_event_id": eid, "season_type": 2, "completed": completed,
                "home_score": home, "away_score": away}

    def test_only_new_or_changed_games(self):
        games = [
            self._game(1),
            self._game(2),
            self._game(3, home=11),
            self._game(4, completed=False),
            self._game(5),
        ]
        prior = {
            2: {"completed": True, "home_score": 10, "away_score": 7,
                "has_summary": False, "has_box": True},
            3: {"completed": True, "home_score": 10, "away_score": 7,
                "has_summary": True, "has_box": True},
            5: {"completed": False, "home_score": None, "away_score": None,
                "has_summary": False, "has_box": False},
        }
        self.assertEqual(backfill.summaries_to_fetch(games, prior), [1, 3, 5])

    def test_concurrency_stays_at_four(self):
        state = {"n": 0, "max": 0}
        lock = threading.Lock()

        def fetch(league, eid):
            with lock:
                state["n"] += 1
                state["max"] = max(state["max"], state["n"])
            time.sleep(0.05)
            with lock:
                state["n"] -= 1
            return {"event": eid}

        got = backfill.map_summaries("cbb", range(9), fetch, backfill.SUMMARY_WORKERS)
        self.assertEqual(backfill.SUMMARY_WORKERS, 4)
        self.assertEqual(len(got), 9)
        self.assertLessEqual(state["max"], 4)
        self.assertGreaterEqual(state["max"], 2)

    def test_partial_failure_keeps_successes(self):
        def fetch(league, eid):
            if eid == 2:
                raise TimeoutError("timed out")
            return {"event": eid}

        with self.assertRaises(backfill.SummaryFetchError) as caught:
            backfill.map_summaries("cfb", [1, 2, 3], fetch, 1)
        self.assertEqual([eid for eid, _ in caught.exception.stored], [1])
        self.assertNotIn("http", str(caught.exception))

    def test_public_error_hides_urls(self):
        self.assertNotIn("http", backfill.public_error(TimeoutError("https://example.test/x")))
        self.assertNotIn("example", backfill.public_error(TimeoutError("https://example.test/x")))


class ManifestMergeTest(unittest.TestCase):
    def _entry(self, path, league, season, sha, rows=1):
        dataset = path.split("/")[1]
        return {"path": path, "league": league, "dataset": dataset, "season": season,
                "sha256": sha, "rows": rows, "columns": [{"name": "game_id", "type": "int"}]}

    def test_merge_keeps_files_this_run_did_not_touch(self):
        remote = {"files": [
            self._entry("nba/games/2026.parquet", "nba", 2026, "aaa"),
            self._entry("cfb/games/2024.parquet", "cfb", 2024, "bbb", 920),
        ], "datasets": {"games": {"leagues": ["nba"], "about": "old"}}}
        changed = self._entry("nba/games/2026.parquet", "nba", 2026, "ccc", 12)
        changed["_diff"] = {"rows_added": 1}
        merged = gh_publish.merge_manifest(remote, [changed], export.dataset_catalog)
        by_path = {f["path"]: f for f in merged["files"]}
        self.assertEqual(by_path["nba/games/2026.parquet"]["sha256"], "ccc")
        self.assertEqual(by_path["cfb/games/2024.parquet"]["sha256"], "bbb")
        self.assertNotIn("_diff", by_path["nba/games/2026.parquet"])
        self.assertEqual(merged["datasets"]["games"]["leagues"], ["nba", "cfb"])
        self.assertIn("College football", merged["datasets"]["games"]["about"])

    def test_reread_picks_up_a_file_added_after_the_first_read(self):
        nba = self._entry("nba/games/2026.parquet", "nba", 2026, "aaa")
        cfb = self._entry("cfb/games/2024.parquet", "cfb", 2024, "bbb")
        state = {"phase": "stale", "files": [nba, cfb]}

        def fetch():
            if state["phase"] == "stale":
                state["phase"] = "fresh"
                return {"files": [dict(nba)], "datasets": {}}
            return {"files": [dict(f) for f in state["files"]], "datasets": {}}

        def upload(manifest):
            state["phase"] = "fresh"
            state["files"] = [dict(f) for f in manifest["files"]]

        changed = self._entry("nba/games/2026.parquet", "nba", 2026, "ccc", 12)
        merged = gh_publish.commit_manifest(
            [changed], export.dataset_catalog, fetch, upload, sleep=lambda _s: None)
        by_path = {f["path"]: f for f in merged["files"]}
        self.assertEqual(by_path["nba/games/2026.parquet"]["sha256"], "ccc")
        self.assertEqual(by_path["cfb/games/2024.parquet"]["sha256"], "bbb")

    def test_retry_when_a_concurrent_upload_lands_during_the_write(self):
        nba = self._entry("nba/games/2026.parquet", "nba", 2026, "aaa")
        cfb = self._entry("cfb/games/2024.parquet", "cfb", 2024, "bbb")
        state = {"files": [nba], "clobber": True}

        def fetch():
            return {"files": [dict(f) for f in state["files"]], "datasets": {}}

        def upload(manifest):
            if state["clobber"]:
                state["clobber"] = False
                state["files"] = [dict(f) for f in manifest["files"]] + [dict(cfb)]
            else:
                state["files"] = [dict(f) for f in manifest["files"]]

        changed = self._entry("nba/games/2026.parquet", "nba", 2026, "ccc", 12)
        merged = gh_publish.commit_manifest(
            [changed], lambda files: {"ok": True}, fetch, upload, sleep=lambda _s: None)
        by_path = {f["path"]: f for f in merged["files"]}
        self.assertEqual(by_path["nba/games/2026.parquet"]["sha256"], "ccc")
        self.assertEqual(by_path["cfb/games/2024.parquet"]["sha256"], "bbb")


class NightlyLeagueTest(unittest.TestCase):
    def _manifest(self, pairs):
        files = []
        for league, season in pairs:
            files.append({"league": league, "dataset": "games", "season": season,
                          "path": f"{league}/games/{season}.parquet"})
        return {"files": files}

    def test_college_waits_for_its_current_season(self):
        pro = [("mlb", 2026), ("nba", 2027), ("nfl", 2026), ("nhl", 2027)]
        only_pro = leagues_to_update(self._manifest(pro), today=TODAY)
        self.assertEqual(only_pro, ["mlb", "nba", "nfl", "nhl"])
        historical = leagues_to_update(
            self._manifest(pro + [("cfb", 2021), ("cbb", 2026)]), today=TODAY)
        self.assertEqual(historical, ["mlb", "nba", "nfl", "nhl"])
        current = leagues_to_update(
            self._manifest(pro + [("cfb", 2026), ("cbb", 2027)]), today=TODAY)
        self.assertEqual(current, ["mlb", "nba", "nfl", "nhl", "cfb", "cbb"])

    def test_skip_when_the_season_is_complete(self):
        manifest = {"files": []}
        for dataset in season_backfill.expected_datasets("cfb"):
            manifest["files"].append({"league": "cfb", "dataset": dataset, "season": 2024})
        self.assertEqual(season_backfill.missing_datasets(manifest, "cfb", 2024), [])
        self.assertIn("games", season_backfill.missing_datasets(manifest, "cfb", 2025))
        self.assertIn("player_game_logs", season_backfill.expected_datasets("cbb"))
        self.assertNotIn("passing_game_logs", season_backfill.expected_datasets("cbb"))


class WorkflowAndFixtureTest(unittest.TestCase):
    def test_backfill_workflow_is_manual_and_separate(self):
        text = (ROOT / ".github" / "workflows" / "backfill.yml").read_text()
        self.assertIn("workflow_dispatch:", text)
        self.assertNotIn("schedule:", text)
        self.assertNotIn("nightly-data", text)
        self.assertIn("season-backfill-", text)
        self.assertIn("cancel-in-progress: false", text)
        nightly = (ROOT / ".github" / "workflows" / "nightly.yml").read_text()
        self.assertIn("nightly-data", nightly)
        self.assertNotIn("season-backfill", nightly)
        blob = text + nightly
        self.assertNotIn("SPORTSLEADER_DISPATCH_TOKEN", blob)
        self.assertNotIn("CRONJOB_ORG_API_KEY", blob)

    def test_fixtures_have_no_urls(self):
        for path in FIXTURES.rglob("*.json"):
            text = path.read_text()
            self.assertNotIn("http://", text, path.name)
            self.assertNotIn("https://", text, path.name)
            self.assertNotIn(".com", text, path.name)


class ExhibitionTest(unittest.TestCase):
    def test_one_game_opponents_stay_in_college_logs(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn = db.connect(Path(tmp) / "t.db")
            db.init_db(conn)
            conn.execute(
                "INSERT INTO teams (team_id, league, espn_id, abbreviation) VALUES "
                "(1, 'cfb', 1, 'A'), (2, 'cfb', 2, 'B'), "
                "(3, 'cbb', 3, 'C'), (4, 'cbb', 4, 'D'), "
                "(5, 'nfl', 5, 'E'), (6, 'nfl', 6, 'F')")
            rows = [
                (1, "cfb", 1, 2), (2, "cbb", 3, 4), (3, "nfl", 5, 6),
            ]
            conn.executemany(
                "INSERT INTO games (espn_event_id, league, season, season_type, "
                "start_time_utc, game_date, home_team_id, away_team_id, completed) "
                "VALUES (?, ?, 2024, 2, '2024-11-16T18:00:00+00:00', '2024-11-16', ?, ?, 1)",
                rows)
            conn.commit()
            self.assertEqual(db.exhibition_team_ids(conn, "cfb"), set())
            self.assertEqual(db.exhibition_team_ids(conn, "cbb"), set())
            self.assertEqual(db.exhibition_team_ids(conn, "nfl"), {5, 6})
            conn.close()


class TeamBoxFixtureTest(unittest.TestCase):
    def test_cfb_team_box_reads_display_values(self):
        summary = _load("cfb", "401628551_summary.json")
        stats = teambox.flat_stats(summary["boxscore"]["teams"], "2483", "cfb")
        self.assertEqual(stats[(None, "totalYards")], "354")
        self.assertEqual(stats[(None, "firstDowns")], "19")
        # yards per play is an NFL column the college box does not send
        self.assertIsNone(stats[(None, "yardsPerPlay")])


if __name__ == "__main__":
    unittest.main()
