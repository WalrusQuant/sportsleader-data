"""Lineup refresh writes NBA and NHL rows for games after 2026-11-03.

The fetch is injected, so the test does not touch the network. A page that
fails is counted and does not discard the rows another page already wrote.
"""

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from bayesratings import db, export
from bayesratings.lineup_refresh import READER, Missing, norm_name, refresh

OLD = "2026-11-02T12:00:00+00:00"
NOW = datetime(2026, 11, 9, 15, 0, tzinfo=timezone.utc)
TODAY = NOW.date()
SLOTS = ("PG", "SG", "SF", "PF", "C")
HOME = [("Alice", "Alpha"), ("Bob", "Beta"), ("Cara", "Gamma"), ("Dan", "Delta"), ("Eve", "Epsilon")]
AWAY = [("Frank", "Foxtrot"), ("Gina", "Golf"), ("Hank", "Hotel"), ("Iris", "India"), ("Jake", "Juliet")]


def _page(props) -> bytes:
    blob = json.dumps({"props": {"pageProps": props}})
    return f'<html><script id="__NEXT_DATA__" type="application/json">{blob}</script></html>'.encode()


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

GOALIES = _page({"data": [{
    "date": "2026-11-08",
    "dateGmt": "2026-11-09T00:00:00Z",
    "homeTeamId": 6, "homeTeamName": "Boston Bruins",
    "homeGoalieId": 11, "homeGoalieName": "Jeremy Swayman",
    "homeNewsStrengthName": "Confirmed", "homeNewsDetails": "starting",
    "awayTeamId": 10, "awayTeamName": "Florida Panthers",
    "awayGoalieId": 12, "awayGoalieName": "Sergei Bobrovsky",
    "awayNewsStrengthName": "Confirmed", "awayNewsDetails": "starting",
}]})


def _lines(team_id, name, abbr, players):
    return _page({
        "combinations": {
            "teamId": team_id, "teamName": name, "teamAbbreviation": abbr,
            "players": players,
        },
        "sortedTeams": [
            {"slug": "boston-bruins", "name": "Boston Bruins", "shortName": "BOS"},
            {"slug": "florida-panthers", "name": "Florida Panthers", "shortName": "FLA"},
            {"slug": "winnipeg-jets", "name": "Winnipeg Jets", "shortName": "WPG"},
        ],
    })


def _skater(pid, name, group, pos):
    return {
        "playerId": pid, "name": name, "groupIdentifier": group,
        "categoryIdentifier": "ev", "positionIdentifier": pos, "injuryStatus": None,
    }


BOSTON = _lines(6, "Boston Bruins", "BOS", [
    _skater(1, "David Pastrnak", "f1", "rw"),
    _skater(2, "Pavel Zacha", "f1", "c"),
])
FLORIDA = _lines(10, "Florida Panthers", "FLA", [
    _skater(3, "Sam Reinhart", "f1", "rw"),
])
WINNIPEG = _lines(52, "Winnipeg Jets", "WPG", [
    _skater(4, "Mark Scheifele", "f1", "c"),
])


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
        self._game(8008, "nhl", "2026-11-08", 3, 4, "2026-11-09T00:00:00Z")
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

    def _game(self, eid, league, day, home, away, start):
        self.conn.execute(
            "INSERT INTO games (espn_event_id, league, season, season_type, start_time_utc, "
            "game_date, home_team_id, away_team_id, completed, neutral_site, covid_2020, "
            "if_necessary, series_completed) VALUES (?,?,2027,2,?,?,?,?,0,0,0,0,0)",
            (eid, league, start, day, home, away))

    def _roster(self, league, tid, aid, name):
        self.conn.execute(
            "INSERT INTO roster_states (league, team_id, athlete_id, name, state_hash, "
            "valid_from, last_seen) VALUES (?,?,?,?, 'h', '2026-10-01T00:00:00+00:00', "
            "'2026-11-01T00:00:00+00:00')", (league, tid, aid, name))

    def _fetch(self, fail_lines=False):
        def fetch(url, accept, headers):
            if "00_daily_lineups_20261107" in url:
                return NBA_JSON
            if "00_daily_lineups_" in url or "depthcharts" in url:
                raise Missing(url)
            if "starting-goalies" in url and not url.startswith(READER):
                raise Missing("403")
            if "starting-goalies" in url:
                return GOALIES
            if fail_lines and "/teams/" in url:
                raise RuntimeError("line page down")
            if "/teams/boston-bruins/" in url:
                return BOSTON
            if "/teams/florida-panthers/" in url:
                return FLORIDA
            if "/teams/winnipeg-jets/" in url:
                return WINNIPEG
            raise Missing(url)
        return fetch

    def test_files_gain_rows_after_nov_3(self):
        before_lines = self.conn.execute("SELECT COUNT(*) FROM nhl_line_states").fetchone()[0]
        result = refresh(self.conn, log=lambda *_: None, today=TODAY, now=NOW, fetch=self._fetch())
        self.assertEqual(result["errors"], 0, result)
        self.assertEqual(result["direct_blocked"], 1)
        self.assertGreaterEqual(result["nba"]["not_posted"], 1)

        home_from = self.conn.execute(
            "SELECT DISTINCT valid_from FROM nba_lineup_states WHERE espn_event_id = 9007 "
            "AND team_id = 1 AND valid_to IS NULL").fetchall()
        self.assertEqual([r[0] for r in home_from], [OLD])
        away = self.conn.execute(
            "SELECT COUNT(*) FROM nba_lineup_states WHERE espn_event_id = 9007 AND team_id = 2 "
            "AND valid_to IS NULL").fetchone()[0]
        self.assertEqual(away, 5)

        kept = self.conn.execute(
            "SELECT valid_from, valid_to FROM nhl_line_states WHERE name = 'David Pastrnak'").fetchone()
        self.assertEqual(kept[0], "2026-11-03T22:00:00+00:00")
        self.assertIsNone(kept[1])
        closed = self.conn.execute(
            "SELECT valid_to FROM nhl_line_states WHERE name = 'Brad Marchand'").fetchone()[0]
        self.assertIsNotNone(closed)
        self.assertGreater(
            self.conn.execute("SELECT COUNT(*) FROM nhl_line_states").fetchone()[0], before_lines)

        goal = self.conn.execute(
            "SELECT game_date, espn_event_id FROM nhl_goalie_states WHERE goalie_name = 'Jeremy Swayman'"
        ).fetchone()
        self.assertEqual(goal[0], "2026-11-08")
        self.assertEqual(goal[1], 8008)

        self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        out = Path(self.tmp.name) / "export"
        export.run(str(self.db_path), out, leagues=("nba", "nhl"),
                   datasets={"lineups", "probables"}, log=lambda *_: None)
        nba = pd.read_parquet(out / "nba/lineups/2027.parquet")
        after = nba["game_date"].astype(str).str[:10]
        self.assertTrue((after >= "2026-11-04").any())
        self.assertIn("2026-11-07", set(after))
        nhl = pd.read_parquet(out / "nhl/lineups/2027.parquet")
        self.assertGreater(len(nhl), before_lines)
        fresh = nhl[nhl["player_name"] == "Pavel Zacha"]
        self.assertEqual(len(fresh), 1)
        self.assertGreaterEqual(str(fresh["valid_from"].iloc[0])[:10], "2026-11-04")
        prob = pd.read_parquet(out / "nhl/probables/2027.parquet")
        dfo = prob[(prob["source"] == "daily_faceoff") & (prob["game_date"].astype(str).str[:10] >= "2026-11-04")]
        self.assertGreaterEqual(len(dfo), 2)

    def test_line_failure_does_not_drop_nba_rows(self):
        result = refresh(self.conn, log=lambda *_: None, today=TODAY, now=NOW,
                         fetch=self._fetch(fail_lines=True))
        self.assertGreaterEqual(result["errors"], 1)
        away = self.conn.execute(
            "SELECT COUNT(*) FROM nba_lineup_states WHERE team_id = 2 AND valid_to IS NULL").fetchone()[0]
        self.assertEqual(away, 5)
        # The published Boston lines are untouched when the line pages fail.
        open_lines = self.conn.execute(
            "SELECT COUNT(*) FROM nhl_line_states WHERE valid_to IS NULL").fetchone()[0]
        self.assertEqual(open_lines, 2)

    def test_norm_name_collapses_initials(self):
        self.assertEqual(norm_name("J. J. Peterka"), norm_name("JJ Peterka"))
        self.assertEqual(norm_name("Moore Jr., Wendell"), "wendell moore")


if __name__ == "__main__":
    unittest.main()
