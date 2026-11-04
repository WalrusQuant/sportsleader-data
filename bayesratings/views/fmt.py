"""Constants and small text/number formatters shared by every page.

Prototype copy keeps BOX_STATS verbatim; teambox.extract only imports that map.
"""

BOX_STATS = {
    "nfl": [(None, "totalYards", "total yards / game", 0, True),
            (None, "yardsPerPlay", "yards / play", 1, True),
            (None, "netPassingYards", "passing yards / game", 0, True),
            (None, "rushingYards", "rushing yards / game", 0, True),
            (None, "firstDowns", "first downs / game", 1, True),
            (None, "turnovers", "turnovers / game", 1, False)],
    "nba": [(None, "fieldGoalPct", "FG%", 1, True),
            (None, "threePointFieldGoalPct", "3P%", 1, True),
            (None, "totalRebounds", "rebounds / game", 1, True),
            (None, "assists", "assists / game", 1, True),
            (None, "totalTurnovers", "turnovers / game", 1, False),
            (None, "pointsInPaint", "points in paint / game", 1, True)],
    "nhl": [(None, "shotsTotal", "shots / game", 1, True),
            (None, "faceoffPercent", "faceoff %", 1, True),
            (None, "hits", "hits / game", 1, True),
            (None, "blockedShots", "blocked shots / game", 1, True),
            (None, "takeaways", "takeaways / game", 1, True),
            (None, "giveaways", "giveaways / game", 1, False)],
    "mlb": [("batting", "hits", "hits / game", 1, True),
            ("batting", "homeRuns", "home runs / game", 2, True),
            ("batting", "walks", "walks drawn / game", 1, True),
            ("batting", "strikeouts", "batter strikeouts / game", 1, False),
            ("pitching", "strikeouts", "pitcher strikeouts / game", 1, True),
            ("fielding", "errors", "errors / game", 2, False)],
}
