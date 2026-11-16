"""Lineup refresh writes NBA and NHL rows for games after 2026-11-03.

The fetch is injected, so the test does not touch the network. A page that
fails is counted and does not discard the rows another page already wrote.
NHL goalies and lines come from official JSON fixtures, including one
recorded box score and shift chart.
"""

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from bayesratings import db, export, snapshots
from bayesratings.lineup_refresh import MIN_UNIT_SECONDS, Missing, build_units, norm_name, refresh
from bayesratings.nightly import Teams

OLD = "2026-11-02T12:00:00+00:00"
NOW = datetime(2026, 11, 9, 15, 0, tzinfo=timezone.utc)
TODAY = NOW.date()
SLOTS = ("PG", "SG", "SF", "PF", "C")
HOME = [("Alice", "Alpha"), ("Bob", "Beta"), ("Cara", "Gamma"), ("Dan", "Delta"), ("Eve", "Epsilon")]
AWAY = [("Frank", "Foxtrot"), ("Gina", "Golf"), ("Hank", "Hotel"), ("Iris", "India"), ("Jake", "Juliet")]
BLOCKED = ("dailyfaceoff.com", "r.jina.ai")
FIXTURES = Path(__file__).resolve().parent / "fixtures" / "nhl"
RECORDED_GAME = 2026020326


def _nba_players(people, start_id):
    out = []
    for i, (first, last) in enumerate(people):
        out.append({
            "personId": start_id + i,
            "firstName": first,
            "lastName": last,
            "playerName": f"{first} {last}",
            "position": SLOTS[i],
            "lineupStatus": "Confirmed",
        })
    return out


NBA_JSON = json.dumps({"games": [{
    "gameId": "0022600999",
    "homeTeam": {"teamAbbreviation": "BOS", "players": _nba_players(HOME, 1000)},
    "awayTeam": {"teamAbbreviation": "NYK", "players": _nba_players(AWAY, 2000)},
}]}).encode()


def _skater(pid, name, position, starter=False):
    return {"playerId": pid, "name": {"default": name}, "position": position, "starter": starter}


def _shift(pid, first, last, team, start, end):
    return {
        "playerId": pid, "firstName": first, "lastName": last, "teamAbbrev": team,
        "period": 1, "startTime": start, "endTime": end, "typeCode": 517, "shiftNumber": 1,
    }


# Five minutes of 5-on-5, then two minutes of 5-on-4. The power play must not
# extend the line. Goal events (type 505, zero length) are ignored.
_BOS = [
    (301, "David", "Pastrnak", "RW"),
    (302, "Pavel", "Zacha", "C"),
    (901, "Casey", "Unknown", "C"),
    (902, "Hampus", "Lindholm", "D"),
    (903, "Charlie", "McAvoy", "D"),
]
_FLA = [
    (303, "Sam", "Reinhart", "RW"),
    (904, "Sam", "Bennett", "C"),
    (905, "Matthew", "Tkachuk", "LW"),
    (906, "Aaron", "Ekblad", "D"),
    (907, "Gustav", "Forsling", "D"),
]
SYN_SHIFTS = []
for pid, first, last, _pos in _BOS:
    SYN_SHIFTS.append(_shift(pid, first, last, "BOS", "00:00", "05:00"))
    SYN_SHIFTS.append(_shift(pid, first, last, "BOS", "05:00", "07:00"))
for pid, first, last, _pos in _FLA:
    SYN_SHIFTS.append(_shift(pid, first, last, "FLA", "00:00", "05:00"))
    if last != "Bennett":
        SYN_SHIFTS.append(_shift(pid, first, last, "FLA", "05:00", "07:00"))
SYN_SHIFTS.append({
    "playerId": 301, "firstName": "David", "lastName": "Pastrnak", "teamAbbrev": "BOS",
    "period": 1, "startTime": "04:00", "endTime": "04:00", "typeCode": 505, "shiftNumber": 0,
})
SYN_SHIFTS.append(_shift(311, "Jeremy", "Swayman", "BOS", "00:00", "07:00"))
SYN_SHIFTS.append(_shift(312, "Sergei", "Bobrovsky", "FLA", "00:00", "07:00"))

SYN_BOX = {
    "id": 2026020999,
    "gameState": "OFF",
    "gameDate": "2026-11-08",
    "awayTeam": {"abbrev": "FLA"},
    "homeTeam": {"abbrev": "BOS"},
    "playerByGameStats": {
        "awayTeam": {
            "forwards": [_skater(pid, f"{first} {last}", pos) for pid, first, last, pos in _FLA if pos != "D"],
            "defense": [_skater(pid, f"{first} {last}", pos) for pid, first, last, pos in _FLA if pos == "D"],
            "goalies": [_skater(312, "S. Bobrovsky", "G", starter=True)],
        },
        "homeTeam": {
            "forwards": [_skater(pid, f"{first} {last}", pos) for pid, first, last, pos in _BOS if pos != "D"],
            "defense": [_skater(pid, f"{first} {last}", pos) for pid, first, last, pos in _BOS if pos == "D"],
            "goalies": [_skater(311, "J. Swayman", "G", starter=True)],
        },
    },
}
SYN_SCHEDULE = {"gameWeek": [{"date": "2026-11-08", "games": [{
    "id": 2026020999,
    "gameState": "OFF",
    "startTimeUTC": "2026-11-09T00:00:00Z",
    "awayTeam": {"abbrev": "FLA", "placeName": {"default": "Florida"}, "commonName": {"default": "Panthers"}},
    "homeTeam": {"abbrev": "BOS", "placeName": {"default": "Boston"}, "commonName": {"default": "Bruins"}},
}]}]}


def _dump(payload) -> bytes:
    return json.dumps(payload).encode()


SCHEDULE_JSON = _dump(SYN_SCHEDULE)
BOX_JSON = _dump(SYN_BOX)
SHIFTS_JSON = _dump({"total": len(SYN_SHIFTS), "data": SYN_SHIFTS})


class RefreshTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "state.db"
        self.conn = db.connect(self.db_path)
        db.init_db(self.conn)
        self.conn.isolation_level = None
        self._team(1, "nba", 2, "BOS", "Boston Celtics")
        self._team(2, "nba", 18, "NY", "New York Knicks")
        self._team(3, "nhl", 6, "BOS", "Boston Bruins")
        self._team(4, "nhl", 26, "FLA", "Florida Panthers")
        self._team(5, "nhl", 28, "WPG", "Winnipeg Jets")
        self._game(9001, "nba", "2026-11-03", 1, 2, "2026-11-03T00:00:00Z")
        self._game(9007, "nba", "2026-11-07", 1, 2, "2026-11-07T00:00:00Z")
        self._game(8008, "nhl", "2026-11-08", 3, 4, "2026-11-09T00:00:00Z", completed=1)
        for i, (first, last) in enumerate(HOME):
            self._roster("nba", 1, 101 + i, f"{first} {last}")
        for i, (first, last) in enumerate(AWAY):
            self._roster("nba", 2, 201 + i, f"{first} {last}")
        self._roster("nhl", 3, 301, "David Pastrnak")
        self._roster("nhl", 3, 302, "Pavel Zacha")
        self._roster("nhl", 3, 399, "Brad Marchand")
        self._roster("nhl", 3, 311, "Jeremy Swayman")
        self._roster("nhl", 4, 312, "Sergei Bobrovsky")
        self._roster("nhl", 4, 303, "Sam Reinhart")
        self.conn.execute(
            "INSERT INTO game_probables (espn_event_id, league, team_id, kind, athlete_id, "
            "name, status, raw_json, state_hash, valid_from, last_seen) "
            "VALUES (8008, 'nhl', 3, 'SG', 311, 'Jeremy Swayman', 'Expected', '{}', 'h', ?, ?)",
            (OLD, OLD))
        # Published home five. Same players the fixture will return, so a
        # refresh must not close and rewrite them.
        for i, (first, last) in enumerate(HOME):
            self.conn.execute(
                "INSERT INTO nba_lineup_states (league, espn_event_id, team_id, slot, athlete_id, "
                "name, status, src, raw_json, state_hash, valid_from, last_seen) "
                "VALUES ('nba', 9007, 1, ?, ?, ?, 'Confirmed', 'nba.com', '{}', 'h', ?, ?)",
                (SLOTS[i], 101 + i, f"{first} {last}", OLD, OLD))
        self.conn.execute(
            "INSERT INTO nhl_line_states (league, dfo_team_id, team_id, group_id, category, "
            "dfo_player_id, athlete_id, name, position_id, raw_json, state_hash, valid_from, last_seen) "
            "VALUES ('nhl', 6, 3, 'f1', 'ev', 301, 301, 'David Pastrnak', 'rw', '{}', 'h', ?, ?)",
            ("2026-11-03T22:00:00+00:00", "2026-11-03T22:00:00+00:00"))
        self.conn.execute(
            "INSERT INTO nhl_line_states (league, dfo_team_id, team_id, group_id, category, "
            "dfo_player_id, athlete_id, name, position_id, raw_json, state_hash, valid_from, last_seen) "
            "VALUES ('nhl', 6, 3, 'f1', 'ev', 399, 399, 'Brad Marchand', 'lw', '{}', 'h', ?, ?)",
            ("2026-11-03T22:00:00+00:00", "2026-11-03T22:00:00+00:00"))

    def tearDown(self):
        self.conn.close()
        self.tmp.cleanup()

    def _team(self, tid, league, espn, abbr, name):
        self.conn.execute(
            "INSERT INTO teams (team_id, league, espn_id, abbreviation, name) VALUES (?,?,?,?,?)",
            (tid, league, espn, abbr, name))

    def _game(self, eid, league, day, home, away, start, completed=0):
        self.conn.execute(
            "INSERT INTO games (espn_event_id, league, season, season_type, start_time_utc, "
            "game_date, home_team_id, away_team_id, completed, neutral_site, covid_2020, "
            "if_necessary, series_completed) VALUES (?,?,2027,2,?,?,?,?,?,0,0,0,0)",
            (eid, league, start, day, home, away, completed))

    def _roster(self, league, tid, aid, name):
        self.conn.execute(
            "INSERT INTO roster_states (league, team_id, athlete_id, name, state_hash, "
            "valid_from, last_seen) VALUES (?,?,?,?, 'h', '2026-10-01T00:00:00+00:00', "
            "'2026-11-01T00:00:00+00:00')", (league, tid, aid, name))

    def _fetch(self, fail_nhl=False, calls=None):
        def fetch(url, accept, headers):
            if calls is not None:
                calls.append(url)
            for host in BLOCKED:
                if host in url:
                    raise AssertionError(url)
            if "00_daily_lineups_20261107" in url:
                return NBA_JSON
            if "00_daily_lineups_" in url or "depthcharts" in url:
                raise Missing(url)
            if fail_nhl and "nhle.com" in url:
                raise RuntimeError("nhl down")
            if "/schedule/" in url:
                return SCHEDULE_JSON
            if "/boxscore" in url:
                return BOX_JSON
            if "shiftcharts" in url:
                return SHIFTS_JSON
            raise Missing(url)
        return fetch

    def test_files_gain_rows_after_nov_3(self):
        before_lines = self.conn.execute("SELECT COUNT(*) FROM nhl_line_states").fetchone()[0]
        calls = []
        result = refresh(self.conn, log=lambda *_: None, today=TODAY, now=NOW,
                         fetch=self._fetch(calls=calls))
        self.assertEqual(result["errors"], 0, result)
        self.assertGreaterEqual(result["nba"]["not_posted"], 1)
        self.assertLess(result["nhl_seconds"], 5)
        for url in calls:
            for host in BLOCKED:
                self.assertNotIn(host, url)

        home_from = self.conn.execute(
            "SELECT DISTINCT valid_from FROM nba_lineup_states WHERE espn_event_id = 9007 "
            "AND team_id = 1 AND valid_to IS NULL").fetchall()
        self.assertEqual([r[0] for r in home_from], [OLD])
        away = self.conn.execute(
            "SELECT COUNT(*) FROM nba_lineup_states WHERE espn_event_id = 9007 AND team_id = 2 "
            "AND valid_to IS NULL").fetchone()[0]
        self.assertEqual(away, 5)

        # Older team charts are not a game's lines, so this pass leaves them open.
        kept = self.conn.execute(
            "SELECT valid_from, valid_to FROM nhl_line_states WHERE name = 'Brad Marchand'").fetchone()
        self.assertEqual(kept[0], "2026-11-03T22:00:00+00:00")
        self.assertIsNone(kept[1])
        self.assertGreater(
            self.conn.execute("SELECT COUNT(*) FROM nhl_line_states").fetchone()[0], before_lines)

        goal = self.conn.execute(
            "SELECT game_date, espn_event_id, athlete_id, source, strength "
            "FROM nhl_goalie_states WHERE goalie_name = 'Jeremy Swayman'"
        ).fetchone()
        self.assertEqual(goal[0], "2026-11-08")
        self.assertEqual(goal[1], 8008)
        self.assertEqual(goal[2], 311)
        self.assertEqual(goal[3], "nhl_api")
        self.assertEqual(goal[4], "Confirmed")
        unknown = self.conn.execute(
            "SELECT athlete_id FROM nhl_line_states WHERE name = 'Casey Unknown' AND valid_to IS NULL"
        ).fetchone()
        self.assertIsNotNone(unknown)
        self.assertIsNone(unknown[0])
        self.assertGreaterEqual(result["lines"]["unmatched"], 1)

        nhl_calls = [url for url in calls if "nhle.com" in url]
        self.assertGreaterEqual(len(nhl_calls), 3)
        again = []
        second = refresh(self.conn, log=lambda *_: None, today=TODAY, now=NOW,
                         fetch=self._fetch(calls=again))
        self.assertEqual(second["errors"], 0, second)
        self.assertEqual([url for url in again if "nhle.com" in url], [])
        self.assertEqual(second["goalies"].get("games", 0), 0)

        self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        out = Path(self.tmp.name) / "export"
        export.run(str(self.db_path), out, leagues=("nba", "nhl"),
                   datasets={"lineups", "probables"}, log=lambda *_: None)
        nba = pd.read_parquet(out / "nba/lineups/2027.parquet")
        after = nba["game_date"].astype(str).str[:10]
        self.assertTrue((after >= "2026-11-04").any())
        self.assertIn("2026-11-07", set(after))
        self.assertEqual(
            list(nba.columns),
            [name for name, _typ in export.LINEUP_COLS])
        nhl = pd.read_parquet(out / "nhl/lineups/2027.parquet")
        self.assertEqual(list(nhl.columns), list(nba.columns))
        self.assertGreater(len(nhl), before_lines)
        fresh = nhl[nhl["player_name"] == "Pavel Zacha"]
        self.assertEqual(len(fresh), 1)
        self.assertEqual(fresh["source"].iloc[0], "nhl_api")
        self.assertEqual(int(fresh["game_id"].iloc[0]), 8008)
        self.assertEqual(str(fresh["game_date"].iloc[0])[:10], "2026-11-08")
        self.assertEqual(fresh["slot"].iloc[0], "f1")
        self.assertEqual(fresh["role"].iloc[0], "line_ev")
        missed = nhl[nhl["player_name"] == "Casey Unknown"]
        self.assertEqual(len(missed), 1)
        self.assertTrue(pd.isna(missed["player_id"].iloc[0]))
        prob = pd.read_parquet(out / "nhl/probables/2027.parquet")
        self.assertEqual(list(prob.columns), [name for name, _typ in export.PROB_COLS])
        official = prob[(prob["source"] == "nhl_api") & (prob["game_date"].astype(str).str[:10] >= "2026-11-04")]
        self.assertGreaterEqual(len(official), 2)
        espn = prob[(prob["source"] == "espn") & (prob["player_name"] == "Jeremy Swayman")]
        self.assertEqual(len(espn), 1)
        self.assertEqual(espn["status"].iloc[0], "Expected")

        # A later nightly reloads the file into an empty database. The official
        # starters are already there, so the NHL step does not fetch them again.
        reloaded = Path(self.tmp.name) / "reloaded.db"
        conn = db.connect(reloaded)
        db.init_db(conn)
        teams = Teams(conn)
        snapshots.load(conn, teams, "nhl", "probables", prob)
        snapshots.load(conn, teams, "nhl", "lineups", nhl)
        home = conn.execute(
            "SELECT team_id FROM teams WHERE league = 'nhl' AND abbreviation = 'BOS'").fetchone()[0]
        away_id = conn.execute(
            "SELECT team_id FROM teams WHERE league = 'nhl' AND abbreviation = 'FLA'").fetchone()[0]
        conn.execute(
            "INSERT INTO games (espn_event_id, league, season, season_type, start_time_utc, "
            "game_date, home_team_id, away_team_id, completed, neutral_site, covid_2020, "
            "if_necessary, series_completed) VALUES (8008, 'nhl', 2027, 2, "
            "'2026-11-09T00:00:00Z', '2026-11-08', ?, ?, 1, 0, 0, 0, 0)",
            (home, away_id))
        conn.commit()
        conn.isolation_level = None
        re_calls = []
        re = refresh(conn, log=lambda *_: None, today=TODAY, now=NOW,
                     fetch=self._fetch(calls=re_calls))
        self.assertEqual([url for url in re_calls if "nhle.com" in url], [], re)
        conn.close()

    def test_line_failure_does_not_drop_nba_rows(self):
        result = refresh(self.conn, log=lambda *_: None, today=TODAY, now=NOW,
                         fetch=self._fetch(fail_nhl=True))
        self.assertGreaterEqual(result["errors"], 1)
        away = self.conn.execute(
            "SELECT COUNT(*) FROM nba_lineup_states WHERE team_id = 2 AND valid_to IS NULL").fetchone()[0]
        self.assertEqual(away, 5)
        # The published Boston lines are untouched when the NHL fetch fails.
        open_lines = self.conn.execute(
            "SELECT COUNT(*) FROM nhl_line_states WHERE valid_to IS NULL").fetchone()[0]
        self.assertEqual(open_lines, 2)

    def test_norm_name_collapses_initials(self):
        self.assertEqual(norm_name("J. J. Peterka"), norm_name("JJ Peterka"))
        self.assertEqual(norm_name("Moore Jr., Wendell"), "wendell moore")

    def test_build_units_counts_five_on_five_only(self):
        positions = {}
        for pid, first, last, pos in _BOS + _FLA:
            kind = "D" if pos == "D" else "F"
            positions[pid] = {"kind": kind, "position_id": pos.lower() if pos != "RW" else "rw",
                              "name": f"{first} {last}"}
        positions[301]["position_id"] = "rw"
        positions[302]["position_id"] = "c"
        positions[901]["position_id"] = "c"
        for pid, pos in ((303, "rw"), (904, "c"), (905, "lw"), (906, "d"), (907, "d"),
                         (902, "d"), (903, "d")):
            positions[pid]["position_id"] = pos
        positions[311] = {"kind": "G", "position_id": "g", "name": "Jeremy Swayman"}
        positions[312] = {"kind": "G", "position_id": "g", "name": "Sergei Bobrovsky"}
        units = build_units(SYN_SHIFTS, positions, min_seconds=MIN_UNIT_SECONDS)
        bos = units["BOS"]
        self.assertEqual(len(bos["lines"]), 1)
        self.assertEqual(bos["lines"][0]["seconds"], 300)
        self.assertEqual(
            [player["name"] for player in bos["lines"][0]["players"]],
            ["Casey Unknown", "Pavel Zacha", "David Pastrnak"])
        self.assertEqual(bos["pairs"][0]["seconds"], 300)
        self.assertEqual(
            {player["player_id"] for player in bos["pairs"][0]["players"]},
            {902, 903})
        short = build_units(SYN_SHIFTS, positions, min_seconds=301)
        self.assertEqual(short.get("BOS", {}).get("lines", []), [])

    def test_nothing_calls_blocked_hosts(self):
        root = Path(__file__).resolve().parents[1] / "bayesratings"
        for path in root.rglob("*.py"):
            text = path.read_text()
            for host in BLOCKED:
                self.assertNotIn(host, text, f"{path} mentions {host}")
        calls = []
        refresh(self.conn, log=lambda *_: None, today=TODAY, now=NOW,
                fetch=self._fetch(calls=calls))
        self.assertTrue(any("nhle.com" in url for url in calls))
        for url in calls:
            for host in BLOCKED:
                self.assertNotIn(host, url)


class RecordedNhlTest(unittest.TestCase):
    """Box score and shift chart saved from one final game."""

    def test_recorded_game_lines_and_starters(self):
        box = json.loads((FIXTURES / f"{RECORDED_GAME}_boxscore.json").read_text())
        shifts = json.loads((FIXTURES / f"{RECORDED_GAME}_shifts.json").read_text())
        from bayesratings.lineup_refresh import box_roster
        units = build_units(shifts, box_roster(box))
        edm = {tuple(player["name"] for player in unit["players"]): unit["seconds"]
               for unit in units["EDM"]["lines"]}
        self.assertEqual(edm[("Connor McDavid", "Leon Draisaitl", "Vasily Podkolzin")], 445)
        pairs = {tuple(player["name"] for player in unit["players"]): unit["seconds"]
                 for unit in units["EDM"]["pairs"]}
        self.assertEqual(pairs[("Evan Bouchard", "Mattias Ekholm")], 712)

        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        conn = db.connect(Path(tmp.name) / "state.db")
        self.addCleanup(conn.close)
        db.init_db(conn)
        conn.isolation_level = None
        conn.execute(
            "INSERT INTO teams (team_id, league, espn_id, abbreviation, name) "
            "VALUES (1, 'nhl', 22, 'EDM', 'Edmonton Oilers'), "
            "(2, 'nhl', 4, 'PHI', 'Philadelphia Flyers')")
        conn.execute(
            "INSERT INTO games (espn_event_id, league, season, season_type, start_time_utc, "
            "game_date, home_team_id, away_team_id, completed, neutral_site, covid_2020, "
            "if_necessary, series_completed) VALUES (88015, 'nhl', 2027, 2, "
            "'2026-11-16T00:30:00Z', '2026-11-15', 2, 1, 1, 0, 0, 0, 0)")
        conn.execute(
            "INSERT INTO roster_states (league, team_id, athlete_id, name, state_hash, "
            "valid_from, last_seen) VALUES ('nhl', 1, 4242, 'Tristan Jarry', 'h', "
            "'2026-10-01T00:00:00+00:00', '2026-11-01T00:00:00+00:00')")
        schedule = {"gameWeek": [{"date": "2026-11-15", "games": [{
            "id": RECORDED_GAME,
            "gameState": "OFF",
            "startTimeUTC": "2026-11-16T00:30:00Z",
            "awayTeam": {"abbrev": "EDM", "placeName": {"default": "Edmonton"},
                         "commonName": {"default": "Oilers"}},
            "homeTeam": {"abbrev": "PHI", "placeName": {"default": "Philadelphia"},
                         "commonName": {"default": "Flyers"}},
        }]}]}

        def fetch(url, accept, headers):
            for host in BLOCKED:
                if host in url:
                    raise AssertionError(url)
            if "/schedule/" in url:
                return _dump(schedule)
            if "/boxscore" in url:
                return _dump(box)
            if "shiftcharts" in url:
                return _dump(shifts)
            raise Missing(url)

        today = datetime(2026, 11, 16, tzinfo=timezone.utc).date()
        now = datetime(2026, 11, 16, 12, tzinfo=timezone.utc)
        result = refresh(conn, log=lambda *_: None, today=today, now=now, fetch=fetch)
        self.assertEqual(result["errors"], 0, result)
        starters = {
            row[0]: row[1] for row in conn.execute(
                "SELECT goalie_name, athlete_id FROM nhl_goalie_states WHERE source = 'nhl_api'")
        }
        self.assertEqual(set(starters), {"Tristan Jarry", "Dan Vladar"})
        self.assertEqual(starters["Tristan Jarry"], 4242)
        self.assertIsNone(starters["Dan Vladar"])
        names = [row[0] for row in conn.execute(
            "SELECT name FROM nhl_line_states WHERE team_id = 1 AND group_id = ("
            "SELECT group_id FROM nhl_line_states WHERE name = 'Connor McDavid' "
            "AND team_id = 1 AND espn_event_id = 88015 AND valid_to IS NULL) "
            "AND espn_event_id = 88015 AND valid_to IS NULL ORDER BY name")]
        self.assertEqual(names, ["Connor McDavid", "Leon Draisaitl", "Vasily Podkolzin"])
        pod = conn.execute(
            "SELECT athlete_id FROM nhl_line_states WHERE name = 'Vasily Podkolzin'").fetchone()
        self.assertIsNone(pod[0])
        calls = []

        def fetch_again(url, accept, headers):
            calls.append(url)
            return fetch(url, accept, headers)

        refresh(conn, log=lambda *_: None, today=today, now=now, fetch=fetch_again)
        self.assertEqual(calls, [])


if __name__ == "__main__":
    unittest.main()
