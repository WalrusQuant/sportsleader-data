-- bayesRatings storage schema (SQLite).
-- Base ratings site: games in, ratings out. One writer (the daily pipeline),
-- read-mostly. Keep it small; add tables when something consumes them.

PRAGMA foreign_keys = ON;

-- Canonical teams. Our own team_id; keyed on ESPN's franchise-stable numeric id
-- so relocations/renames don't break joins (A's = espn_id 11 in both Oakland and
-- "ATH"). Name/abbrev/colors/logo are display attributes that can change.
CREATE TABLE IF NOT EXISTS teams (
    team_id         INTEGER PRIMARY KEY,
    league          TEXT    NOT NULL,          -- mlb | nfl | nba | nhl
    espn_id         INTEGER NOT NULL,
    location        TEXT,
    name            TEXT,                       -- displayName
    abbreviation    TEXT,
    color           TEXT,
    alternate_color TEXT,
    logo            TEXT,
    -- canonical franchise for ratings continuity across relocations where ESPN
    -- issued a new id (NHL Arizona 24 -> Utah 129764). NULL = itself.
    franchise_team_id INTEGER REFERENCES teams (team_id),
    UNIQUE (league, espn_id)
);

-- Games: the model's input plus provenance. espn_event_id is the natural unique
-- key (distinct per doubleheader game). Re-scraping upserts on it, so backfills
-- are idempotent and results fill in as games complete.
CREATE TABLE IF NOT EXISTS games (
    game_id         INTEGER PRIMARY KEY,
    espn_event_id   INTEGER NOT NULL UNIQUE,
    league          TEXT    NOT NULL,
    season          INTEGER NOT NULL,           -- per-sport convention (see docs/espn-reference)
    season_type     INTEGER,                    -- 1 pre, 2 regular, 3 post
    start_time_utc  TEXT    NOT NULL,           -- ISO UTC, minutes precision
    game_date       TEXT    NOT NULL,           -- America/Chicago calendar date (slate key)
    home_team_id    INTEGER NOT NULL REFERENCES teams (team_id),
    away_team_id    INTEGER NOT NULL REFERENCES teams (team_id),
    home_score      INTEGER,                    -- NULL until completed
    away_score      INTEGER,
    completed       INTEGER NOT NULL DEFAULT 0, -- 0/1
    status          TEXT,                       -- STATUS_FINAL, etc.
    venue           TEXT,
    venue_city      TEXT,                       -- ESPN venue.address.city
    venue_state     TEXT,                       -- ESPN venue.address.state
    venue_country   TEXT,                       -- ESPN venue.address.country (often absent)
    neutral_site    INTEGER NOT NULL DEFAULT 0,
    ending          TEXT,                       -- NHL: regulation | ot | shootout
    covid_2020      INTEGER NOT NULL DEFAULT 0, -- pandemic-affected season flag
    notes           TEXT,                       -- ESPN event notes, e.g. 'NLCS - Game 5 If Necessary'
    if_necessary    INTEGER NOT NULL DEFAULT 0, -- playoff game played only if the series is still open
    series_summary  TEXT,                       -- e.g. 'MIL wins series 4-0'
    series_completed INTEGER NOT NULL DEFAULT 0 -- ESPN says the series is decided
);

CREATE INDEX IF NOT EXISTS idx_games_league_season ON games (league, season);
CREATE INDEX IF NOT EXISTS idx_games_date          ON games (game_date);
CREATE INDEX IF NOT EXISTS idx_games_home          ON games (home_team_id);
CREATE INDEX IF NOT EXISTS idx_games_away          ON games (away_team_id);

-- Every scrape/ingest run logged (standing rule: no untracked production paths).
CREATE TABLE IF NOT EXISTS scraper_runs (
    run_id         INTEGER PRIMARY KEY,
    league         TEXT,
    mode           TEXT,                        -- backfill | daily | ingest
    started_at     TEXT,
    finished_at    TEXT,
    games_written  INTEGER,
    status         TEXT,                        -- ok | error
    notes          TEXT
);

-- Backfill progress: one row per (league, scoreboard date). A date marked done
-- with all its completed games summarized is skipped on rerun.
CREATE TABLE IF NOT EXISTS backfill_dates (
    league      TEXT NOT NULL,
    date        TEXT NOT NULL,                  -- YYYY-MM-DD as requested
    events      INTEGER,
    status      TEXT,                           -- done | error
    fetched_at  TEXT,
    PRIMARY KEY (league, date)
);

-- Raw ESPN game summary (box score, team stats, leaders, pickcenter lines),
-- gzip-compressed JSON. One per event; extract fields later without refetching.
CREATE TABLE IF NOT EXISTS game_summaries (
    espn_event_id INTEGER PRIMARY KEY,
    league        TEXT NOT NULL,
    fetched_at    TEXT NOT NULL,
    raw_bytes     INTEGER,
    json_gz       BLOB NOT NULL
);

-- Each team's identity and main home venue per season (relocations/renames).
CREATE TABLE IF NOT EXISTS team_seasons (
    team_id       INTEGER NOT NULL REFERENCES teams (team_id),
    league        TEXT NOT NULL,
    season        INTEGER NOT NULL,
    name          TEXT,
    abbreviation  TEXT,
    home_venue    TEXT,
    home_games    INTEGER,
    PRIMARY KEY (team_id, season)
);

-- Daily model output. One set of rows per (snapshot_date, league); a rerun of
-- the same day deletes and rewrites that day's rows in one transaction.
CREATE TABLE IF NOT EXISTS rating_snapshots (
    snapshot_date TEXT    NOT NULL,             -- America/Chicago run date
    league        TEXT    NOT NULL,
    season        INTEGER NOT NULL,             -- rating season used
    team_id       INTEGER NOT NULL REFERENCES teams (team_id),
    rating        REAL    NOT NULL,             -- zero-centered posterior mean
    rating_sd     REAL,                         -- posterior sd (centered draws)
    rating_q05    REAL,
    rating_q95    REAL,
    home_adv      REAL,                         -- league posterior mean, logit
    backfilled    INTEGER NOT NULL DEFAULT 0,   -- 1 = rebuilt later by replay.py
    PRIMARY KEY (snapshot_date, league, team_id)
);

CREATE TABLE IF NOT EXISTS forecast_snapshots (
    snapshot_date   TEXT    NOT NULL,
    league          TEXT    NOT NULL,
    espn_event_id   INTEGER NOT NULL,
    game_date       TEXT,
    home_team_id    INTEGER REFERENCES teams (team_id),
    away_team_id    INTEGER REFERENCES teams (team_id),
    home_prob       REAL,
    away_prob       REAL,
    home_spread     REAL,                        -- negative = home favored
    total_pred      REAL,
    home_total_pred REAL,
    away_total_pred REAL,
    backfilled      INTEGER NOT NULL DEFAULT 0,  -- 1 = rebuilt later by replay.py
    -- starter adjustment audit (MLB pitcher / NHL goalie; NULL = not applied)
    home_starter_id  INTEGER,                    -- ESPN athlete id used
    away_starter_id  INTEGER,
    home_starter_src TEXT,                       -- espn | mlbapi | dfo (":nohist" = no shift)
    away_starter_src TEXT,
    home_starter_rel REAL,                       -- vs team's usual starter (runs/goals)
    away_starter_rel REAL,
    home_prob_base   REAL,                       -- win chance before the adjustment
    total_pred_base  REAL,                       -- total before the adjustment
    -- player-level adjustment audit (player_adj.py; NULL = not applied)
    home_player_x    REAL,                       -- vs team's usual players (MLB: lineup)
    away_player_x    REAL,
    home_player_x2   REAL,                       -- MLB bullpen (NULL elsewhere)
    away_player_x2   REAL,
    home_player_id   INTEGER,                    -- NFL starting QB used
    away_player_id   INTEGER,
    home_player_src  TEXT,                       -- report | lines | roster | depth | last | posted | usual
    away_player_src  TEXT,
    player_prob_base REAL,                       -- win chance before the player term (after starters)
    player_spread_base REAL,
    player_total_base  REAL,
    player_home_total_base REAL,
    player_away_total_base REAL,
    player_inputs    TEXT,                       -- fingerprint of the pre-game inputs used
    -- win-chance stack audit (bayes_elo.py; NULL on older and replayed rows)
    p_logit          REAL,                       -- logit win chance (after starters + players)
    p_elo            REAL,                       -- dynamic Bayesian Elo (NULL = fallback)
    elo_weight       REAL,                       -- home_prob = (1-w)*p_logit + w*p_elo
    model_version    TEXT,
    PRIMARY KEY (snapshot_date, espn_event_id)
);

-- Latest forecast for games AFTER the snapshot day, replaced per league on
-- every daily run. Display only: the scorecard and the Kalshi comparison
-- grade forecast_snapshots (the game-day forecast), never this table.
CREATE TABLE IF NOT EXISTS upcoming_forecasts (
    snapshot_date   TEXT    NOT NULL,            -- the run that made it
    league          TEXT    NOT NULL,
    espn_event_id   INTEGER NOT NULL PRIMARY KEY,
    game_date       TEXT,
    home_team_id    INTEGER REFERENCES teams (team_id),
    away_team_id    INTEGER REFERENCES teams (team_id),
    home_prob       REAL,
    away_prob       REAL,
    home_spread     REAL,
    total_pred      REAL,
    home_total_pred REAL,
    away_total_pred REAL,
    -- starter adjustment audit (MLB pitcher / NHL goalie; NULL = not applied)
    home_starter_id  INTEGER,                    -- ESPN athlete id used
    away_starter_id  INTEGER,
    home_starter_src TEXT,                       -- espn | mlbapi | dfo (":nohist" = no shift)
    away_starter_src TEXT,
    home_starter_rel REAL,                       -- vs team's usual starter (runs/goals)
    away_starter_rel REAL,
    home_prob_base   REAL,                       -- win chance before the adjustment
    total_pred_base  REAL,                       -- total before the adjustment
    -- player-level adjustment audit (player_adj.py; NULL = not applied)
    home_player_x    REAL,                       -- vs team's usual players (MLB: lineup)
    away_player_x    REAL,
    home_player_x2   REAL,                       -- MLB bullpen (NULL elsewhere)
    away_player_x2   REAL,
    home_player_id   INTEGER,                    -- NFL starting QB used
    away_player_id   INTEGER,
    home_player_src  TEXT,                       -- report | lines | roster | depth | last | posted | usual
    away_player_src  TEXT,
    player_prob_base REAL,                       -- win chance before the player term (after starters)
    player_spread_base REAL,
    player_total_base  REAL,
    player_home_total_base REAL,
    player_away_total_base REAL,
    player_inputs    TEXT,                       -- fingerprint of the pre-game inputs used
    -- win-chance stack audit (bayes_elo.py)
    p_logit          REAL,
    p_elo            REAL,
    elo_weight       REAL,
    model_version    TEXT
);

CREATE TABLE IF NOT EXISTS daily_runs (
    run_id        INTEGER PRIMARY KEY,
    snapshot_date TEXT,
    league        TEXT,
    started_at    TEXT,
    finished_at   TEXT,
    status        TEXT,                          -- ok | skipped | error
    rating_season INTEGER,
    n_games       INTEGER,                       -- completed games given to the model
    n_teams       INTEGER,
    n_forecasts   INTEGER,
    seconds       REAL,
    notes         TEXT
);

-- Kalshi game-winner market prices (read-only public market data), matched to
-- ESPN games by teams and date. Prices are dollars per $1 contract (0-1).
-- A Kalshi price is a market price, not a probability.
CREATE TABLE IF NOT EXISTS kalshi_quotes (
    snapshot_date TEXT    NOT NULL,
    league        TEXT    NOT NULL,
    espn_event_id INTEGER NOT NULL,
    ticker        TEXT    NOT NULL,
    team_id       INTEGER REFERENCES teams (team_id),  -- the YES side
    yes_bid       REAL,
    yes_ask       REAL,
    last          REAL,
    fetched_at    TEXT    NOT NULL,
    PRIMARY KEY (snapshot_date, ticker)
);

-- Every change in a Kalshi quote (for charting price moves and for scoring
-- against the pre-game price). kalshi_quotes holds only the latest.
CREATE TABLE IF NOT EXISTS kalshi_quote_history (
    fetched_at    TEXT    NOT NULL,
    snapshot_date TEXT    NOT NULL,             -- game date (US/Eastern)
    league        TEXT    NOT NULL,
    espn_event_id INTEGER NOT NULL,
    ticker        TEXT    NOT NULL,
    team_id       INTEGER,
    yes_bid       REAL,
    yes_ask       REAL,
    last          REAL,
    PRIMARY KEY (ticker, fetched_at)
);
CREATE INDEX IF NOT EXISTS idx_kqh_event ON kalshi_quote_history (espn_event_id, fetched_at);

-- Kalshi PRE-GAME close per game: the 1-minute candle ending at the ESPN
-- start time (or the last earlier candle), never an in-game or settled
-- price. home_prob is the market's implied chance with the vig removed:
-- home_mid / (home_mid + away_mid). It is a market price, not the true
-- probability. source: candles (Kalshi API) or history (our own polls).
-- flag: ok, wide (bid/ask spread > 10c), stale (quote > 30 min before the
-- start), one_sided (a side has no bid or no ask; mid not reliable).
CREATE TABLE IF NOT EXISTS kalshi_closes (
    espn_event_id  INTEGER PRIMARY KEY,
    league         TEXT    NOT NULL,
    game_date      TEXT    NOT NULL,             -- US/Eastern
    start_time_utc TEXT    NOT NULL,             -- the ESPN start used
    home_ticker    TEXT,
    away_ticker    TEXT,
    home_bid REAL, home_ask REAL, away_bid REAL, away_ask REAL,
    home_mid REAL, away_mid REAL,
    home_prob      REAL,                         -- vig removed
    quote_ts       TEXT,                         -- UTC time of the quote used
    source         TEXT    NOT NULL,
    flag           TEXT    NOT NULL,
    fetched_at     TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_kc_league ON kalshi_closes (league, game_date);

-- Prediction-market prices from Kalshi (spreads/totals; its moneyline stays in
-- kalshi_quotes), Polymarket US, ProphetX and Novig: read-only market data,
-- display only, never fed into the model. One row per game, venue, market
-- and line, replaced on each poll. Games are polled only before they start,
-- so a started or finished game keeps its last pre-game prices.
-- market: ml | spread | total. line: 0 (ml), the HOME team's line (spread,
-- -1.5 = home gives 1.5), the points line (total). a = home (ml, spread) or
-- over (total); b = away or under. Prices are dollars per $1 payout (0-1).
-- fee: taker fee coefficient (>= 0: theta*p*(1-p); < 0: share of winnings).
CREATE TABLE IF NOT EXISTS market_quotes (
    espn_event_id INTEGER NOT NULL,
    league        TEXT    NOT NULL,
    venue         TEXT    NOT NULL,             -- kalshi | polymarket | prophetx | novig
    market        TEXT    NOT NULL,
    line          REAL    NOT NULL,
    a_bid REAL, a_ask REAL, b_bid REAL, b_ask REAL,
    fee           REAL,
    ref           TEXT,                         -- venue's market id / slug / ticker
    fetched_at    TEXT    NOT NULL,             -- UTC
    PRIMARY KEY (espn_event_id, venue, market, line)
);

-- Every change in a venue's quotes (append-only history of market_quotes,
-- which keeps only the latest): a row is written when a polled (game,
-- venue, market, line) is new or its prices changed, so an unchanged quote
-- between polls is implied. Kalshi's moneyline history is kalshi_quote_history.
CREATE TABLE IF NOT EXISTS market_quote_history (
    espn_event_id INTEGER NOT NULL,
    league        TEXT    NOT NULL,
    venue         TEXT    NOT NULL,
    market        TEXT    NOT NULL,
    line          REAL    NOT NULL,
    a_bid REAL, a_ask REAL, b_bid REAL, b_ask REAL,
    fetched_at    TEXT    NOT NULL,             -- UTC
    PRIMARY KEY (espn_event_id, venue, market, line, fetched_at)
);

-- Market Dynamics pre-game lock (bayesratings/market_dynamics.py): the
-- official prediction, frozen just before the scheduled start, never
-- replaced. p_md = inv_logit(logit(p_mkt) + k*(logit(p_model) - logit(p_mkt))).
-- margin/total: m_md = m_mkt + g*(m_model - m_mkt), g per league and kind.
-- venues: JSON {venue: vig-removed home chance}. source: blend | model (no
-- market price: our model alone).
CREATE TABLE IF NOT EXISTS market_dynamics_snapshots (
    espn_event_id  INTEGER PRIMARY KEY,
    league         TEXT    NOT NULL,
    start_time_utc TEXT,
    snapshot_at    TEXT    NOT NULL,             -- UTC
    p_md           REAL    NOT NULL,             -- home chance
    p_model        REAL    NOT NULL,
    p_mkt          REAL,
    k              REAL,
    venues         TEXT,
    source         TEXT    NOT NULL,
    -- expected HOME margin and total: blended, ours, market consensus (NULL
    -- market = no venue line; locks before 2026-10-18 12:30 CT have none)
    margin_md      REAL, total_md REAL, margin_model REAL, total_model REAL,
    margin_mkt     REAL, total_mkt REAL,
    line_venues    TEXT                          -- JSON {"spread": {venue: median}, "total": {...}}
);

-- Last success / failure per venue and league (shown when a feed is down).
CREATE TABLE IF NOT EXISTS market_venue_status (
    venue   TEXT NOT NULL,
    league  TEXT NOT NULL,
    ok_at   TEXT,
    err_at  TEXT,
    error   TEXT,
    note    TEXT,
    PRIMARY KEY (venue, league)
);

-- ---------------------------------------------------------------- players
-- Player-level box lines extracted from the stored ESPN game summaries
-- (game_summaries.json_gz) by bayesratings/players.py. No ESPN requests; the
-- daily job extracts new finals after their summaries are fetched. Data only:
-- nothing here feeds the model yet. athlete_id = ESPN athlete id. Every
-- per-game table has the same leading columns (event, game, team, player)
-- plus typed key stats; stats_json keeps the raw box line as {ESPN key: text}.
-- Only completed, playable (not removed/postponed), non-exhibition games.

CREATE TABLE IF NOT EXISTS players (
    league          TEXT    NOT NULL,
    athlete_id      INTEGER NOT NULL,
    name            TEXT,
    short_name      TEXT,
    position        TEXT,                       -- latest abbreviation seen (NFL boxes carry none)
    team_id         INTEGER REFERENCES teams (team_id),   -- team in his latest game
    first_game_date TEXT,
    last_game_date  TEXT,
    games           INTEGER,                    -- distinct games with a box line
    PRIMARY KEY (league, athlete_id)
);

-- Team history: one row per player, season and team he appeared for.
CREATE TABLE IF NOT EXISTS player_team_seasons (
    league          TEXT    NOT NULL,
    athlete_id      INTEGER NOT NULL,
    season          INTEGER NOT NULL,
    team_id         INTEGER NOT NULL REFERENCES teams (team_id),
    games           INTEGER NOT NULL,
    first_game_date TEXT,
    last_game_date  TEXT,
    PRIMARY KEY (league, athlete_id, season, team_id)
);

-- MLB batting: everyone in the batting box, in box order.
CREATE TABLE IF NOT EXISTS player_game_mlb_batting (
    espn_event_id INTEGER NOT NULL, season INTEGER NOT NULL, season_type INTEGER,
    game_date TEXT NOT NULL, team_id INTEGER NOT NULL, opp_team_id INTEGER NOT NULL,
    is_home INTEGER NOT NULL, athlete_id INTEGER NOT NULL, position TEXT,
    starter INTEGER NOT NULL,                   -- in the starting lineup
    bat_order INTEGER,                          -- lineup slot 1-9 (subs share their slot)
    box_order INTEGER NOT NULL,                 -- 1-based order within the team's box
    ab INTEGER, r INTEGER, h INTEGER, rbi INTEGER, hr INTEGER, bb INTEGER, k INTEGER,
    pitches INTEGER,                            -- pitches seen
    stats_json TEXT NOT NULL,
    PRIMARY KEY (espn_event_id, team_id, athlete_id)
);

-- MLB pitching. pitch_order 1 = first pitcher listed = the starter.
CREATE TABLE IF NOT EXISTS player_game_mlb_pitching (
    espn_event_id INTEGER NOT NULL, season INTEGER NOT NULL, season_type INTEGER,
    game_date TEXT NOT NULL, team_id INTEGER NOT NULL, opp_team_id INTEGER NOT NULL,
    is_home INTEGER NOT NULL, athlete_id INTEGER NOT NULL, position TEXT,
    starter INTEGER NOT NULL,                   -- pitch_order = 1
    pitch_order INTEGER NOT NULL,
    decision TEXT,                              -- W | L | S | H | BS (ESPN note)
    outs INTEGER,                               -- innings pitched as outs (IP 5.2 = 17)
    h INTEGER, r INTEGER, er INTEGER, bb INTEGER, k INTEGER, hr INTEGER,
    pitches INTEGER, strikes INTEGER,
    stats_json TEXT NOT NULL,
    PRIMARY KEY (espn_event_id, team_id, athlete_id)
);

-- NBA: every listed player, including did-not-play (stats NULL).
CREATE TABLE IF NOT EXISTS player_game_nba (
    espn_event_id INTEGER NOT NULL, season INTEGER NOT NULL, season_type INTEGER,
    game_date TEXT NOT NULL, team_id INTEGER NOT NULL, opp_team_id INTEGER NOT NULL,
    is_home INTEGER NOT NULL, athlete_id INTEGER NOT NULL, position TEXT,
    starter INTEGER NOT NULL,
    did_not_play INTEGER NOT NULL,
    dnp_reason TEXT,                            -- only when did_not_play
    ejected INTEGER,
    minutes INTEGER, pts INTEGER, fgm INTEGER, fga INTEGER, fg3m INTEGER, fg3a INTEGER,
    ftm INTEGER, fta INTEGER, reb INTEGER, oreb INTEGER, dreb INTEGER, ast INTEGER,
    tov INTEGER, stl INTEGER, blk INTEGER, pf INTEGER, plus_minus INTEGER,
    stats_json TEXT NOT NULL,
    PRIMARY KEY (espn_event_id, team_id, athlete_id)
);

-- NHL skaters (forwards and defense). Times in seconds.
CREATE TABLE IF NOT EXISTS player_game_nhl_skater (
    espn_event_id INTEGER NOT NULL, season INTEGER NOT NULL, season_type INTEGER,
    game_date TEXT NOT NULL, team_id INTEGER NOT NULL, opp_team_id INTEGER NOT NULL,
    is_home INTEGER NOT NULL, athlete_id INTEGER NOT NULL, position TEXT,
    unit TEXT NOT NULL,                         -- forwards | defenses | skaters
    toi_sec INTEGER, pp_toi_sec INTEGER, sh_toi_sec INTEGER, es_toi_sec INTEGER,
    shifts INTEGER, g INTEGER, a INTEGER, shots INTEGER, shots_missed INTEGER,
    plus_minus INTEGER, hits INTEGER, blocks INTEGER, takeaways INTEGER,
    giveaways INTEGER, fo_won INTEGER, fo_lost INTEGER, pim INTEGER, so_goals INTEGER,
    stats_json TEXT NOT NULL,
    PRIMARY KEY (espn_event_id, team_id, athlete_id)
);

-- NHL goalies. starter = most time on ice for his team (ties: first listed).
-- ESPN's box has no goalie decision (W/L/OTL).
CREATE TABLE IF NOT EXISTS player_game_nhl_goalie (
    espn_event_id INTEGER NOT NULL, season INTEGER NOT NULL, season_type INTEGER,
    game_date TEXT NOT NULL, team_id INTEGER NOT NULL, opp_team_id INTEGER NOT NULL,
    is_home INTEGER NOT NULL, athlete_id INTEGER NOT NULL, position TEXT,
    starter INTEGER NOT NULL,
    goalie_order INTEGER NOT NULL,              -- order listed in the box
    toi_sec INTEGER, sa INTEGER, ga INTEGER, sv INTEGER, sv_pct REAL,
    es_sv INTEGER, pp_sv INTEGER, sh_sv INTEGER, so_sa INTEGER, so_sv INTEGER, pim INTEGER,
    stats_json TEXT NOT NULL,
    PRIMARY KEY (espn_event_id, team_id, athlete_id)
);

-- NFL passing. qb_starter = most attempts for his team (ties: first listed).
CREATE TABLE IF NOT EXISTS player_game_nfl_passing (
    espn_event_id INTEGER NOT NULL, season INTEGER NOT NULL, season_type INTEGER,
    game_date TEXT NOT NULL, team_id INTEGER NOT NULL, opp_team_id INTEGER NOT NULL,
    is_home INTEGER NOT NULL, athlete_id INTEGER NOT NULL,
    qb_starter INTEGER NOT NULL,
    cmp INTEGER, att INTEGER, yds INTEGER, td INTEGER, ints INTEGER,
    sacks INTEGER, sack_yds INTEGER, qbr REAL, rating REAL,
    stats_json TEXT NOT NULL,
    PRIMARY KEY (espn_event_id, team_id, athlete_id)
);

CREATE TABLE IF NOT EXISTS player_game_nfl_rushing (
    espn_event_id INTEGER NOT NULL, season INTEGER NOT NULL, season_type INTEGER,
    game_date TEXT NOT NULL, team_id INTEGER NOT NULL, opp_team_id INTEGER NOT NULL,
    is_home INTEGER NOT NULL, athlete_id INTEGER NOT NULL,
    car INTEGER, yds INTEGER, td INTEGER, long INTEGER,
    stats_json TEXT NOT NULL,
    PRIMARY KEY (espn_event_id, team_id, athlete_id)
);

CREATE TABLE IF NOT EXISTS player_game_nfl_receiving (
    espn_event_id INTEGER NOT NULL, season INTEGER NOT NULL, season_type INTEGER,
    game_date TEXT NOT NULL, team_id INTEGER NOT NULL, opp_team_id INTEGER NOT NULL,
    is_home INTEGER NOT NULL, athlete_id INTEGER NOT NULL,
    rec INTEGER, tgt INTEGER, yds INTEGER, td INTEGER, long INTEGER,
    stats_json TEXT NOT NULL,
    PRIMARY KEY (espn_event_id, team_id, athlete_id)
);

-- NFL defense, fumbles, interceptions, kick/punt returns, kicking, punting:
-- one row per player and category; stats in stats_json (json_extract them).
CREATE TABLE IF NOT EXISTS player_game_nfl_other (
    espn_event_id INTEGER NOT NULL, season INTEGER NOT NULL, season_type INTEGER,
    game_date TEXT NOT NULL, team_id INTEGER NOT NULL, opp_team_id INTEGER NOT NULL,
    is_home INTEGER NOT NULL, athlete_id INTEGER NOT NULL,
    category TEXT NOT NULL,
    stats_json TEXT NOT NULL,
    PRIMARY KEY (espn_event_id, team_id, athlete_id, category)
);

-- One row per game summary processed: makes the extractor resumable and
-- re-extracts when the summary is refetched or the parser version changes.
-- status: ok | no_box (summary has no player box) | error (see note).
CREATE TABLE IF NOT EXISTS player_extract_log (
    espn_event_id      INTEGER PRIMARY KEY,
    league             TEXT    NOT NULL,
    summary_fetched_at TEXT    NOT NULL,
    extracted_at       TEXT    NOT NULL,
    version            INTEGER NOT NULL,
    status             TEXT    NOT NULL,
    rows               INTEGER NOT NULL DEFAULT 0,
    note               TEXT
);

-- Team Comparison box stats per (game, team), from the stored summary
-- (bayesratings/teambox.py). Display only. stats_json NULL = unreadable box.
CREATE TABLE IF NOT EXISTS team_game_box (
    espn_event_id      INTEGER NOT NULL,
    team_id            INTEGER NOT NULL,
    league             TEXT    NOT NULL,
    summary_fetched_at TEXT    NOT NULL,
    version            TEXT    NOT NULL,          -- hash of the BOX_STATS keys
    extracted_at       TEXT    NOT NULL,
    stats_json         TEXT,
    PRIMARY KEY (espn_event_id, team_id)
);

-- "How they play" counts per (game, team), from the stored summary
-- (bayesratings/style.py): NFL drives / scoring drives / red zone, NBA
-- possessions and minutes, NHL power plays. Display only. NULL = not in the box.
CREATE TABLE IF NOT EXISTS team_style_game (
    espn_event_id      INTEGER NOT NULL,
    team_id            INTEGER NOT NULL,
    league             TEXT    NOT NULL,
    season             INTEGER NOT NULL,
    season_type        INTEGER,
    summary_fetched_at TEXT    NOT NULL,
    version            INTEGER NOT NULL,
    drives INTEGER, scoring_drives INTEGER, rz_td INTEGER, rz_att INTEGER,
    poss REAL, minutes INTEGER, pp_goals INTEGER, pp_opps INTEGER,
    PRIMARY KEY (espn_event_id, team_id)
);

-- Team Lab raw counts per (game, team), from the stored summary
-- (bayesratings/labstats.py). Display only: one row per team per game,
-- counts_json is [for, against] in COUNTS order (null side = unreadable
-- box). Against is the opponent's for-counts from the same summary.
-- Nothing here feeds the model.
CREATE TABLE IF NOT EXISTS team_game_stats (
    espn_event_id      INTEGER NOT NULL,
    team_id            INTEGER NOT NULL,
    opp_team_id        INTEGER NOT NULL,
    league             TEXT    NOT NULL,
    season             INTEGER NOT NULL,
    season_type        INTEGER,
    game_date          TEXT    NOT NULL,
    is_home            INTEGER NOT NULL,
    points_for         INTEGER,
    points_against     INTEGER,
    summary_fetched_at TEXT    NOT NULL,
    version            INTEGER NOT NULL,
    counts_json        TEXT,
    PRIMARY KEY (espn_event_id, team_id)
);

CREATE INDEX IF NOT EXISTS idx_team_game_stats_season
    ON team_game_stats (league, season, game_date);

-- player lookups: one player's games, one team's games
CREATE INDEX IF NOT EXISTS idx_pg_mlb_batting_athlete ON player_game_mlb_batting (athlete_id, game_date);
CREATE INDEX IF NOT EXISTS idx_pg_mlb_batting_team ON player_game_mlb_batting (team_id, game_date);
CREATE INDEX IF NOT EXISTS idx_pg_mlb_pitching_athlete ON player_game_mlb_pitching (athlete_id, game_date);
CREATE INDEX IF NOT EXISTS idx_pg_mlb_pitching_team ON player_game_mlb_pitching (team_id, game_date);
CREATE INDEX IF NOT EXISTS idx_pg_nba_athlete ON player_game_nba (athlete_id, game_date);
CREATE INDEX IF NOT EXISTS idx_pg_nba_team ON player_game_nba (team_id, game_date);
CREATE INDEX IF NOT EXISTS idx_pg_nhl_skater_athlete ON player_game_nhl_skater (athlete_id, game_date);
CREATE INDEX IF NOT EXISTS idx_pg_nhl_skater_team ON player_game_nhl_skater (team_id, game_date);
CREATE INDEX IF NOT EXISTS idx_pg_nhl_goalie_athlete ON player_game_nhl_goalie (athlete_id, game_date);
CREATE INDEX IF NOT EXISTS idx_pg_nhl_goalie_team ON player_game_nhl_goalie (team_id, game_date);
CREATE INDEX IF NOT EXISTS idx_pg_nfl_passing_athlete ON player_game_nfl_passing (athlete_id, game_date);
CREATE INDEX IF NOT EXISTS idx_pg_nfl_passing_team ON player_game_nfl_passing (team_id, game_date);
CREATE INDEX IF NOT EXISTS idx_pg_nfl_rushing_athlete ON player_game_nfl_rushing (athlete_id, game_date);
CREATE INDEX IF NOT EXISTS idx_pg_nfl_rushing_team ON player_game_nfl_rushing (team_id, game_date);
CREATE INDEX IF NOT EXISTS idx_pg_nfl_receiving_athlete ON player_game_nfl_receiving (athlete_id, game_date);
CREATE INDEX IF NOT EXISTS idx_pg_nfl_receiving_team ON player_game_nfl_receiving (team_id, game_date);
CREATE INDEX IF NOT EXISTS idx_pg_nfl_other_athlete ON player_game_nfl_other (athlete_id, game_date);
CREATE INDEX IF NOT EXISTS idx_pg_nfl_other_team ON player_game_nfl_other (team_id, game_date);
CREATE INDEX IF NOT EXISTS idx_player_team_seasons_team ON player_team_seasons (league, team_id, season);

-- ---------------------------------------------------------------------------
-- Pre-game data logging (bayesratings/prelog.py). LOGGING ONLY: nothing reads
-- these for the model, forecasts or pages yet.
--
-- Change-only tables carry one row per distinct state: valid_from = first
-- fetch that saw it, last_seen = latest fetch that still saw it, valid_to =
-- the fetch that saw it change or disappear (NULL = current). State at T:
--   valid_from <= T AND (valid_to IS NULL OR valid_to > T)
-- state_hash covers the typed columns; raw_json is ESPN's object (links and
-- images dropped) as first seen in that state. Times are ISO UTC.

CREATE TABLE IF NOT EXISTS game_probables (     -- from scoreboards (hourly + 6 AM + pre-game)
    id            INTEGER PRIMARY KEY,
    espn_event_id INTEGER NOT NULL,
    league        TEXT    NOT NULL,
    team_id       INTEGER NOT NULL,
    kind          TEXT    NOT NULL,             -- SP (MLB pitcher), SG (NHL goalie), ...
    athlete_id    INTEGER,
    name          TEXT,
    status        TEXT,                         -- NHL: Expected | Confirmed
    raw_json      TEXT,
    state_hash    TEXT    NOT NULL,
    valid_from    TEXT    NOT NULL,
    valid_to      TEXT,
    last_seen     TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_game_probables_event ON game_probables (espn_event_id, valid_to);

CREATE TABLE IF NOT EXISTS game_odds (          -- ESPN's betting block per provider
    id            INTEGER PRIMARY KEY,
    espn_event_id INTEGER NOT NULL,
    league        TEXT    NOT NULL,
    provider_id   INTEGER NOT NULL,
    provider      TEXT,
    details       TEXT,                         -- e.g. 'TB -114'
    over_under    REAL,
    home_ml INTEGER, away_ml INTEGER, home_ml_open INTEGER, away_ml_open INTEGER,
    home_spread REAL, home_spread_odds INTEGER, away_spread_odds INTEGER, home_spread_open REAL,
    total REAL, over_odds INTEGER, under_odds INTEGER, total_open REAL,
    home_favorite INTEGER,
    raw_json      TEXT,
    state_hash    TEXT    NOT NULL,
    valid_from    TEXT    NOT NULL,
    valid_to      TEXT,
    last_seen     TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_game_odds_event ON game_odds (espn_event_id, valid_to);

CREATE TABLE IF NOT EXISTS game_weather (
    id            INTEGER PRIMARY KEY,
    espn_event_id INTEGER NOT NULL,
    league        TEXT    NOT NULL,
    source        TEXT    NOT NULL,             -- scoreboard | summary
    condition TEXT, condition_id INTEGER, temperature INTEGER, high_temp INTEGER,
    low_temp INTEGER, gust INTEGER, precipitation INTEGER,
    raw_json      TEXT,
    state_hash    TEXT    NOT NULL,
    valid_from    TEXT    NOT NULL,
    valid_to      TEXT,
    last_seen     TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_game_weather_event ON game_weather (espn_event_id, valid_to);

CREATE TABLE IF NOT EXISTS roster_states (      -- per-team roster endpoint
    id            INTEGER PRIMARY KEY,
    league        TEXT    NOT NULL,
    team_id       INTEGER NOT NULL,
    athlete_id    INTEGER NOT NULL,
    name          TEXT,
    position      TEXT,
    jersey        TEXT,
    roster_group  TEXT,                         -- ESPN grouping: Pitchers, offense, injuredReserveOrOut, ...
    status        TEXT,                         -- Active, Day-To-Day, Practice Squad, ...
    injury_status TEXT,                         -- Out, Questionable, Injured Reserve, 10-Day-IL, ...
    injury_date   TEXT,
    raw_json      TEXT,
    state_hash    TEXT    NOT NULL,
    valid_from    TEXT    NOT NULL,
    valid_to      TEXT,
    last_seen     TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_roster_states_team ON roster_states (league, team_id, valid_to);
CREATE INDEX IF NOT EXISTS idx_roster_states_athlete ON roster_states (league, athlete_id, valid_from);

CREATE TABLE IF NOT EXISTS injury_details (     -- core API, fetched once per new roster injury
    id            INTEGER PRIMARY KEY,
    league        TEXT    NOT NULL,
    injury_id     INTEGER NOT NULL,
    team_id       INTEGER,
    athlete_id    INTEGER NOT NULL,
    status        TEXT,
    injury_date   TEXT,
    type          TEXT,                         -- body part, e.g. Back
    detail        TEXT,                         -- e.g. Inflammation
    side          TEXT,
    return_date   TEXT,
    fantasy_status TEXT,
    short_comment TEXT,
    raw_json      TEXT,
    state_hash    TEXT    NOT NULL,
    valid_from    TEXT    NOT NULL,
    valid_to      TEXT,
    last_seen     TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_injury_details_athlete ON injury_details (league, athlete_id, valid_to);
CREATE INDEX IF NOT EXISTS idx_injury_details_id ON injury_details (league, injury_id, valid_to);

CREATE TABLE IF NOT EXISTS injury_lookups (     -- which roster injury states we've looked up
    league        TEXT    NOT NULL,
    athlete_id    INTEGER NOT NULL,
    roster_status TEXT    NOT NULL,
    roster_injury_date TEXT,
    injury_id     INTEGER,                      -- NULL: ESPN's core list had no entry
    checked_at    TEXT    NOT NULL,
    PRIMARY KEY (league, athlete_id, roster_status, roster_injury_date)
);

CREATE TABLE IF NOT EXISTS nfl_depth_states (
    id            INTEGER PRIMARY KEY,
    league        TEXT    NOT NULL,             -- always nfl
    team_id       INTEGER NOT NULL,
    formation     TEXT    NOT NULL,             -- e.g. '3WR 1TE', 'Base 4-3 D', 'Special Teams'
    slot          TEXT    NOT NULL,             -- ESPN key: qb, rb, wr1, lde, ...
    depth         INTEGER NOT NULL,             -- 1 = starter
    athlete_id    INTEGER,
    name          TEXT,
    position      TEXT,
    raw_json      TEXT,
    state_hash    TEXT    NOT NULL,
    valid_from    TEXT    NOT NULL,
    valid_to      TEXT,
    last_seen     TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_nfl_depth_team ON nfl_depth_states (team_id, valid_to);

CREATE TABLE IF NOT EXISTS transactions (       -- append-only, deduplicated
    league        TEXT    NOT NULL,
    txn_key       TEXT    NOT NULL,             -- hash of date | team | description
    txn_date      TEXT,
    espn_team_id  INTEGER,
    team_id       INTEGER,
    description   TEXT,
    raw_json      TEXT,
    first_seen    TEXT    NOT NULL,
    PRIMARY KEY (league, txn_key)
);
CREATE INDEX IF NOT EXISTS idx_transactions_team ON transactions (team_id, txn_date);

CREATE TABLE IF NOT EXISTS pregame_snapshots (  -- one per game per pre-game fetch
    espn_event_id INTEGER NOT NULL,
    fetched_at    TEXT    NOT NULL,
    league        TEXT    NOT NULL,
    start_time_utc TEXT   NOT NULL,             -- start as we had it then
    minutes_before INTEGER,
    status        TEXT,
    n_lineup      INTEGER,
    n_injuries    INTEGER,
    weather_condition_id INTEGER, temperature INTEGER, gust INTEGER, precipitation INTEGER,
    raw_bytes     INTEGER,
    summary_gz    BLOB,                         -- summary JSON minus news/videos/standings
    PRIMARY KEY (espn_event_id, fetched_at)
);
CREATE INDEX IF NOT EXISTS idx_pregame_snapshots_league ON pregame_snapshots (league, fetched_at);

CREATE TABLE IF NOT EXISTS pregame_lineups (
    espn_event_id INTEGER NOT NULL,
    fetched_at    TEXT    NOT NULL,
    team_id       INTEGER NOT NULL,
    role          TEXT    NOT NULL,             -- lineup | bench | starter | probable | inactive
    athlete_id    INTEGER NOT NULL,
    name          TEXT,
    order_no      INTEGER,                      -- MLB batting order
    position      TEXT,
    status        TEXT,                         -- probable status / DNP reason
    group_name    TEXT,                         -- box group (NHL forwards/goalies, ...)
    raw_json      TEXT,
    PRIMARY KEY (espn_event_id, fetched_at, team_id, role, athlete_id)
);

CREATE TABLE IF NOT EXISTS pregame_injuries (
    espn_event_id INTEGER NOT NULL,
    fetched_at    TEXT    NOT NULL,
    team_id       INTEGER NOT NULL,
    athlete_id    INTEGER NOT NULL,
    name          TEXT,
    status        TEXT,
    injury_date   TEXT,
    type          TEXT,
    detail        TEXT,
    side          TEXT,
    return_date   TEXT,
    raw_json      TEXT,
    PRIMARY KEY (espn_event_id, fetched_at, team_id, athlete_id)
);

CREATE TABLE IF NOT EXISTS prelog_runs (
    id            INTEGER PRIMARY KEY,
    kind          TEXT    NOT NULL,             -- scoreboard | rosters | depth | transactions | pregame
    league        TEXT    NOT NULL,
    started_at    TEXT    NOT NULL,
    finished_at   TEXT    NOT NULL,
    requests      INTEGER,                      -- NULL for scoreboard logging (no extra requests)
    changed       INTEGER,
    errors        INTEGER NOT NULL DEFAULT 0,
    counts_json   TEXT,
    note          TEXT
);
CREATE INDEX IF NOT EXISTS idx_prelog_runs_kind ON prelog_runs (kind, league, finished_at);

-- ---------------------------------------------------------------------------
-- Gap sources (bayesratings/sources, 2026-10-17): logging only, nothing here
-- feeds the model, forecasts or pages. Change-only *_states tables follow the
-- prelog convention (valid_from / valid_to / last_seen / state_hash).

-- raw payloads, one row per distinct body per (source, key); last_seen moves
-- while the body stays the same
CREATE TABLE IF NOT EXISTS src_raw (
    id            INTEGER PRIMARY KEY,
    source        TEXT    NOT NULL,
    raw_key       TEXT    NOT NULL,
    content_hash  TEXT    NOT NULL,
    fetched_at    TEXT    NOT NULL,
    last_seen     TEXT    NOT NULL,
    content_type  TEXT,
    n_bytes       INTEGER NOT NULL,
    body_gz       BLOB    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_src_raw_key ON src_raw (source, raw_key, fetched_at);

CREATE TABLE IF NOT EXISTS src_meta (           -- robots.txt cache etc.
    key        TEXT PRIMARY KEY,
    value      TEXT,
    updated_at TEXT NOT NULL
);

-- external id -> ESPN id (players, teams, games). espn_id NULL = looked up,
-- no match (see ext_unmatched); method: xref | name_team | name_box | name_league | static
-- | xref_unverified (MLB xref not an id ESPN has given us, no name match)
CREATE TABLE IF NOT EXISTS ext_ids (
    source     TEXT    NOT NULL,                -- mlbam | dfo | nbainj | nflverse | venue
    league     TEXT    NOT NULL,
    kind       TEXT    NOT NULL,                -- player | team | game
    ext_id     TEXT    NOT NULL,
    espn_id    INTEGER,                         -- ESPN athlete / team espn_id / event id
    team_id    INTEGER,                         -- our team_id (teams table) when known
    name       TEXT,
    method     TEXT,
    first_seen TEXT    NOT NULL,
    last_seen  TEXT    NOT NULL,
    xref_id    INTEGER,                         -- mlbam: MLB's raw "espn" xref (may not be an ESPN athlete id)
    PRIMARY KEY (source, league, kind, ext_id)
);

-- everything we could not map to ESPN, never dropped silently
CREATE TABLE IF NOT EXISTS ext_unmatched (
    source      TEXT    NOT NULL,
    league      TEXT    NOT NULL,
    kind        TEXT    NOT NULL,               -- player | team | game | venue
    ext_key     TEXT    NOT NULL,
    name        TEXT,
    team        TEXT,
    context     TEXT,
    first_seen  TEXT    NOT NULL,
    last_seen   TEXT    NOT NULL,
    n_seen      INTEGER NOT NULL DEFAULT 1,
    resolved_at TEXT,
    PRIMARY KEY (source, league, kind, ext_key)
);

-- per-game passes (MLB lineup polls, NFL inactives, NBA starters, weather):
-- which game was looked at when, and whether the data was there
CREATE TABLE IF NOT EXISTS src_game_passes (
    source        TEXT    NOT NULL,
    espn_event_id INTEGER NOT NULL,
    phase         TEXT    NOT NULL,
    fetched_at    TEXT    NOT NULL,
    found         INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (source, espn_event_id, phase, fetched_at)
);

-- MLB Stats API: 40-man + every non-active status (IL etc.) on the full roster
CREATE TABLE IF NOT EXISTS mlb_roster_states (
    id            INTEGER PRIMARY KEY,
    league        TEXT    NOT NULL DEFAULT 'mlb',
    mlb_team_id   INTEGER NOT NULL,
    team_id       INTEGER,
    mlb_person_id INTEGER NOT NULL,
    athlete_id    INTEGER,                      -- ESPN athlete id (via MLB xref)
    name          TEXT,
    position      TEXT,
    jersey        TEXT,
    status_code   TEXT,                         -- A, 40M, D10, D15, D60, RM, ...
    status_desc   TEXT,
    on_40man      INTEGER,
    raw_json      TEXT,
    state_hash    TEXT    NOT NULL,
    valid_from    TEXT    NOT NULL,
    valid_to      TEXT,
    last_seen     TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_mlb_roster_open ON mlb_roster_states (mlb_team_id, valid_to);
CREATE INDEX IF NOT EXISTS idx_mlb_roster_person ON mlb_roster_states (mlb_person_id, valid_from);

CREATE TABLE IF NOT EXISTS mlb_transactions (
    txn_id          INTEGER PRIMARY KEY,         -- MLB transaction id
    txn_date        TEXT,
    effective_date  TEXT,
    resolution_date TEXT,
    type_code       TEXT,
    type_desc       TEXT,
    description     TEXT,                        -- carries the injury: "... 15-day injured list. Right wrist inflammation."
    mlb_person_id   INTEGER,
    athlete_id      INTEGER,
    from_mlb_team   INTEGER,
    to_mlb_team     INTEGER,
    team_id         INTEGER,
    il_related      INTEGER NOT NULL DEFAULT 0,
    first_seen      TEXT    NOT NULL,
    last_seen       TEXT    NOT NULL,
    raw_json        TEXT
);
CREATE INDEX IF NOT EXISTS idx_mlb_tx_person ON mlb_transactions (mlb_person_id, txn_date);

-- MLB gamePk <-> ESPN event, and when each side's lineup was first seen
CREATE TABLE IF NOT EXISTS mlb_games (
    game_pk        INTEGER PRIMARY KEY,
    espn_event_id  INTEGER,
    official_date  TEXT,
    start_time_utc TEXT,
    home_mlb_team  INTEGER,
    away_mlb_team  INTEGER,
    venue_id       INTEGER,
    game_number    INTEGER,
    status         TEXT,
    lineup_home_at TEXT,
    lineup_away_at TEXT,
    weather_json   TEXT,                         -- MLB's own game weather once posted
    first_seen     TEXT    NOT NULL,
    last_seen      TEXT    NOT NULL
);

-- batting order slots 1-9 and slot 0 = probable / starting pitcher
CREATE TABLE IF NOT EXISTS mlb_lineup_states (
    id            INTEGER PRIMARY KEY,
    league        TEXT    NOT NULL DEFAULT 'mlb',
    game_pk       INTEGER NOT NULL,
    espn_event_id INTEGER,
    side          TEXT    NOT NULL,             -- home | away
    slot          INTEGER NOT NULL,
    team_id       INTEGER,
    mlb_person_id INTEGER,
    athlete_id    INTEGER,
    name          TEXT,
    position      TEXT,
    raw_json      TEXT,
    state_hash    TEXT    NOT NULL,
    valid_from    TEXT    NOT NULL,
    valid_to      TEXT,
    last_seen     TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_mlb_lineup_game ON mlb_lineup_states (game_pk, valid_to);
CREATE INDEX IF NOT EXISTS idx_mlb_lineup_event ON mlb_lineup_states (espn_event_id);

-- Daily Faceoff starting goalies: one state per (date, team)
CREATE TABLE IF NOT EXISTS nhl_goalie_states (
    id              INTEGER PRIMARY KEY,
    league          TEXT    NOT NULL DEFAULT 'nhl',
    game_date       TEXT    NOT NULL,           -- DFO's date
    dfo_team_id     INTEGER NOT NULL,
    team_name       TEXT,
    team_id         INTEGER,
    espn_event_id   INTEGER,
    game_time_utc   TEXT,
    side            TEXT,
    dfo_goalie_id   INTEGER,
    goalie_name     TEXT,
    athlete_id      INTEGER,
    strength        TEXT,                       -- Confirmed | Likely | Unconfirmed
    news_created_at TEXT,                       -- when DFO posted the status (UTC)
    news_details    TEXT,
    source_name     TEXT,
    source_url      TEXT,
    raw_json        TEXT,
    state_hash      TEXT    NOT NULL,
    valid_from      TEXT    NOT NULL,
    valid_to        TEXT,
    last_seen       TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_nhl_goalie_date ON nhl_goalie_states (game_date, valid_to);
CREATE INDEX IF NOT EXISTS idx_nhl_goalie_event ON nhl_goalie_states (espn_event_id);

-- Daily Faceoff line combinations: one state per (team, unit, player)
CREATE TABLE IF NOT EXISTS nhl_line_states (
    id             INTEGER PRIMARY KEY,
    league         TEXT    NOT NULL DEFAULT 'nhl',
    dfo_team_id    INTEGER NOT NULL,
    team_id        INTEGER,
    group_id       TEXT    NOT NULL,            -- f1..f4, d1..d3, g, pp1, pp2, pk1, pk2, ir
    category       TEXT,                        -- ev | pp | pk | oi
    dfo_player_id  INTEGER NOT NULL,
    name           TEXT,
    athlete_id     INTEGER,
    position_id    TEXT,                        -- lw c rw ld rd g1 g2 sk1..
    injury_status  TEXT,
    gtd            INTEGER,
    dfo_updated_at TEXT,
    dfo_source     TEXT,
    raw_json       TEXT,
    state_hash     TEXT    NOT NULL,
    valid_from     TEXT    NOT NULL,
    valid_to       TEXT,
    last_seen      TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_nhl_line_team ON nhl_line_states (dfo_team_id, valid_to);

-- NBA official injury report PDFs (one row per report fetched)
CREATE TABLE IF NOT EXISTS nba_injury_reports (
    report_slot   TEXT PRIMARY KEY,             -- 'YYYY-MM-DD HH:MM' US/Eastern
    report_ts_utc TEXT,                         -- the report's own title time, in UTC
    url           TEXT    NOT NULL,
    fetched_at    TEXT    NOT NULL,
    backfill      INTEGER NOT NULL DEFAULT 0,
    n_rows        INTEGER,
    n_unmatched   INTEGER,
    text_sha1     TEXT,
    text_gz       BLOB,                         -- extracted words by line (always)
    pdf_gz        BLOB                          -- the PDF itself, when kept
);

CREATE TABLE IF NOT EXISTS nba_injury_states (
    id            INTEGER PRIMARY KEY,
    league        TEXT    NOT NULL DEFAULT 'nba',
    game_date     TEXT    NOT NULL,             -- report's game date (ET)
    team_name     TEXT    NOT NULL,
    player_key    TEXT    NOT NULL,             -- 'Last, First' as printed, or '(NOT YET SUBMITTED)'
    team_id       INTEGER,
    matchup       TEXT,
    game_time     TEXT,
    espn_event_id INTEGER,
    athlete_id    INTEGER,
    player_name   TEXT,                         -- First Last
    status        TEXT,                         -- Out | Doubtful | Questionable | Probable | Available | NOT YET SUBMITTED
    reason        TEXT,
    report_slot   TEXT,
    raw_json      TEXT,
    state_hash    TEXT    NOT NULL,
    valid_from    TEXT    NOT NULL,
    valid_to      TEXT,
    last_seen     TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_nba_inj_date ON nba_injury_states (game_date, valid_to);
CREATE INDEX IF NOT EXISTS idx_nba_inj_event ON nba_injury_states (espn_event_id);

-- nflverse injuries (official report: practice participation + game status),
-- logged daily so Wed/Thu/Fri changes are kept here
CREATE TABLE IF NOT EXISTS nfl_injury_report_states (
    id                        INTEGER PRIMARY KEY,
    league                    TEXT    NOT NULL DEFAULT 'nfl',
    season                    INTEGER NOT NULL,
    game_type                 TEXT,
    week                      INTEGER NOT NULL,
    team                      TEXT    NOT NULL, -- nflverse abbreviation
    gsis_id                   TEXT    NOT NULL,
    team_id                   INTEGER,
    athlete_id                INTEGER,
    full_name                 TEXT,
    position                  TEXT,
    report_primary_injury     TEXT,
    report_secondary_injury   TEXT,
    report_status             TEXT,
    practice_primary_injury   TEXT,
    practice_secondary_injury TEXT,
    practice_status           TEXT,
    raw_json                  TEXT,
    state_hash                TEXT    NOT NULL,
    valid_from                TEXT    NOT NULL,
    valid_to                  TEXT,
    last_seen                 TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_nfl_injrep_week ON nfl_injury_report_states (season, week, valid_to);

CREATE TABLE IF NOT EXISTS nfl_weekly_roster_states (
    id          INTEGER PRIMARY KEY,
    league      TEXT    NOT NULL DEFAULT 'nfl',
    season      INTEGER NOT NULL,
    week        INTEGER NOT NULL,
    gsis_id     TEXT    NOT NULL,
    game_type   TEXT,
    team        TEXT,
    team_id     INTEGER,
    espn_id     INTEGER,
    athlete_id  INTEGER,
    full_name   TEXT,
    position    TEXT,
    status      TEXT,                           -- ACT, INA (game-day inactive), RES, DEV, CUT ...
    status_abbr TEXT,
    raw_json    TEXT,
    state_hash  TEXT    NOT NULL,
    valid_from  TEXT    NOT NULL,
    valid_to    TEXT,
    last_seen   TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_nfl_wroster_week ON nfl_weekly_roster_states (season, week, valid_to);

-- NFL game-day inactives from ESPN's per-team injury list (~60 and ~20 min
-- before kickoff)
CREATE TABLE IF NOT EXISTS nfl_inactives (
    espn_event_id  INTEGER NOT NULL,
    team_id        INTEGER NOT NULL,
    athlete_id     INTEGER NOT NULL,
    fetched_at     TEXT    NOT NULL,
    minutes_before INTEGER,
    injury_id      INTEGER,
    name           TEXT,
    status         TEXT,
    fantasy_status TEXT,
    injury_date    TEXT,                        -- ESPN's timestamp on the entry
    type           TEXT,
    PRIMARY KEY (espn_event_id, team_id, athlete_id, fetched_at)
);

-- NBA starters from ESPN's box score at / just after tip
CREATE TABLE IF NOT EXISTS nba_starters (
    espn_event_id INTEGER NOT NULL,
    team_id       INTEGER NOT NULL,
    athlete_id    INTEGER NOT NULL,
    fetched_at    TEXT    NOT NULL,
    minutes_after INTEGER,
    name          TEXT,
    position      TEXT,
    raw_json      TEXT,
    PRIMARY KEY (espn_event_id, team_id, athlete_id)
);

-- NBA probable starting lineups (sources/nbalineups.py): NBA.com daily lineups
-- (Expected / Confirmed), else ESPN's depth chart. Change-only, one row per
-- (game, team, slot); for the game page card only, not in the forecast.
CREATE TABLE IF NOT EXISTS nba_lineup_states (
    id            INTEGER PRIMARY KEY,
    league        TEXT    NOT NULL DEFAULT 'nba',
    espn_event_id INTEGER NOT NULL,
    team_id       INTEGER NOT NULL,
    slot          TEXT    NOT NULL,             -- PG | SG | SF | PF | C
    athlete_id    INTEGER,                      -- ESPN id (NULL: name not matched)
    name          TEXT,
    status        TEXT,                         -- Expected | Confirmed (NBA.com) | depth chart
    src           TEXT    NOT NULL,             -- nba.com | espn_depth
    nba_person_id INTEGER,
    raw_json      TEXT,
    state_hash    TEXT    NOT NULL,
    valid_from    TEXT    NOT NULL,
    valid_to      TEXT,
    last_seen     TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_nba_lineup_event ON nba_lineup_states (espn_event_id, team_id, valid_to);

-- venues with coordinates (MLB: Stats API; NFL: bayesratings/data/nfl_venues.csv)
CREATE TABLE IF NOT EXISTS venues (
    league         TEXT    NOT NULL,
    venue_key      TEXT    NOT NULL,            -- MLB venue id, NFL normalized name
    name           TEXT    NOT NULL,
    aliases        TEXT,                        -- '|'-separated other names ESPN uses
    mlb_venue_id   INTEGER,
    home_team_id   INTEGER,
    lat            REAL,
    lon            REAL,
    roof           TEXT,                        -- open | retractable | dome | fixed
    azimuth        REAL,                        -- MLB: home plate -> center field bearing
    elevation      REAL,
    country        TEXT,
    source         TEXT,
    nws_office     TEXT,
    nws_x          INTEGER,
    nws_y          INTEGER,
    nws_hourly_url TEXT,
    nws_checked_at TEXT,
    updated_at     TEXT    NOT NULL,
    PRIMARY KEY (league, venue_key)
);

-- hourly forecast for outdoor games: start hour (offset 0) and the next 3
CREATE TABLE IF NOT EXISTS game_weather_fcst (
    espn_event_id    INTEGER NOT NULL,
    fetched_at       TEXT    NOT NULL,
    phase            TEXT    NOT NULL,          -- morning | pregame
    provider         TEXT    NOT NULL,          -- nws | open-meteo
    hour_offset      INTEGER NOT NULL,
    venue_key        TEXT,
    league           TEXT,
    period_start     TEXT,
    temp_f           REAL,
    wind_mph         REAL,
    wind_gust_mph    REAL,
    wind_dir         TEXT,
    wind_deg         REAL,
    humidity         REAL,
    precip_pct       REAL,
    dewpoint_f       REAL,
    short_forecast   TEXT,
    forecast_updated TEXT,
    raw_json         TEXT,
    PRIMARY KEY (espn_event_id, fetched_at, provider, hour_offset)
);

-- Futures data (bayesratings/futures_data.py). Read by the season sim only;
-- nothing on the game-forecast path reads these tables.
-- Conference / division membership per season, from ESPN standings.
CREATE TABLE IF NOT EXISTS team_groups (
    league          TEXT    NOT NULL,
    season          INTEGER NOT NULL,           -- ESPN season.year (league convention)
    team_id         INTEGER NOT NULL REFERENCES teams (team_id),
    conference      TEXT    NOT NULL,           -- e.g. 'Eastern Conference', 'American League'
    conference_abbr TEXT,
    division        TEXT    NOT NULL,           -- e.g. 'Atlantic Division', 'AFC East'
    division_abbr   TEXT,
    fetched_at      TEXT    NOT NULL,           -- ISO UTC
    PRIMARY KEY (league, season, team_id)
);

-- Every regular-season game still to be played, from each team's ESPN
-- schedule, one row per ESPN event. Replaced per league on each fetch.
-- Separate from games on purpose: the forecast window keeps reading games.
CREATE TABLE IF NOT EXISTS future_schedule (
    espn_event_id   INTEGER PRIMARY KEY,
    league          TEXT    NOT NULL,
    season          INTEGER NOT NULL,
    season_type     INTEGER NOT NULL DEFAULT 2,
    start_time_utc  TEXT    NOT NULL,
    game_date       TEXT    NOT NULL,           -- America/Chicago date, as games.game_date
    time_valid      INTEGER NOT NULL DEFAULT 1, -- 0 = start time TBD (NFL flex)
    week            INTEGER,                    -- NFL week number
    home_team_id    INTEGER NOT NULL REFERENCES teams (team_id),
    away_team_id    INTEGER NOT NULL REFERENCES teams (team_id),
    neutral_site    INTEGER NOT NULL DEFAULT 0,
    status          TEXT,
    fetched_at      TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_future_schedule_league ON future_schedule (league, season, game_date);

-- Futures season sim output (bayesratings/futures/store.py): one row per team
-- per day, kept for trend charts. Written only when every sum check passes.
CREATE TABLE IF NOT EXISTS futures_snapshots (
    snapshot_date TEXT    NOT NULL,             -- America/Chicago run date
    league        TEXT    NOT NULL,
    team_id       INTEGER NOT NULL REFERENCES teams (team_id),
    season        INTEGER NOT NULL,
    n_sims        INTEGER NOT NULL,
    model_version TEXT,
    games_left    INTEGER,
    cur_w         INTEGER,
    cur_l         INTEGER,
    cur_otl       INTEGER,                      -- NHL overtime/shootout losses
    mean_w        REAL,
    p10_w         REAL,                         -- 10th-90th percentile of season wins
    p90_w         REAL,
    mean_pts      REAL,                         -- NHL only
    p10_pts       REAL,
    p90_pts       REAL,
    mean_seed     REAL,                         -- average seed when in the playoffs
    p_div         REAL,
    p_playoff     REAL,
    p_playin      REAL,                         -- NBA only
    p_bye         REAL,                         -- NFL / MLB only
    p_round2      REAL,
    p_round3      REAL,
    p_final       REAL,                         -- = won the conference / league
    p_title       REAL,
    PRIMARY KEY (snapshot_date, league, team_id)
);

CREATE INDEX IF NOT EXISTS idx_futures_snap_team ON futures_snapshots (league, team_id, snapshot_date);

-- One row per league per daily futures run, including skipped and failed ones.
CREATE TABLE IF NOT EXISTS futures_runs (
    snapshot_date TEXT    NOT NULL,
    league        TEXT    NOT NULL,
    season        INTEGER,
    status        TEXT    NOT NULL,             -- ok | check_failed | skipped | error
    n_sims        INTEGER,
    seconds       REAL,
    checks        TEXT,                         -- JSON {name: {got, want, ok}}
    notes         TEXT,
    finished_at   TEXT,
    PRIMARY KEY (snapshot_date, league)
);

-- Per-player model values, one row per player per day (player_values.py).
-- Display only: written after the day's forecasts, never read by them.
CREATE TABLE IF NOT EXISTS player_values (
    snapshot_date TEXT    NOT NULL,             -- America/Chicago run date
    league        TEXT    NOT NULL,
    team_id       INTEGER NOT NULL,
    athlete_id    INTEGER NOT NULL,
    role          TEXT    NOT NULL,             -- P | F | D | G | QB | H | SP | RP
    value         REAL,                         -- per game, league units (player_values.py)
    value_lo      REAL,                         -- 90% range
    value_hi      REAL,
    usage         REAL,                         -- minutes / dropbacks / PA / outs per game
    sample        REAL,                         -- decayed sample behind the rate
    win_logit     REAL,                         -- win-term change when he plays vs the fallback
    is_out        INTEGER,                      -- 1 = counted out for the next game
    out_src       TEXT,
    PRIMARY KEY (snapshot_date, league, team_id, athlete_id, role)
);

CREATE INDEX IF NOT EXISTS idx_player_values_team ON player_values (league, team_id, snapshot_date);
