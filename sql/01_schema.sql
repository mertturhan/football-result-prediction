-- leagues
CREATE TABLE IF NOT EXISTS league (
  league_id   TEXT PRIMARY KEY,         -- e.g. 'EPL'
  name        TEXT NOT NULL,
  country     TEXT
);

-- teams
CREATE TABLE IF NOT EXISTS team (
  team_id     TEXT PRIMARY KEY,
  name        TEXT NOT NULL,
  country     TEXT
);


CREATE TABLE IF NOT EXISTS team_alias (
  alias       TEXT PRIMARY KEY,
  team_id     TEXT NOT NULL REFERENCES team(team_id)
);


CREATE TABLE IF NOT EXISTS match (
  match_id      TEXT PRIMARY KEY,       -- fbref id or your hash
  league_id     TEXT NOT NULL REFERENCES league(league_id),
  season        TEXT NOT NULL,          -- e.g. '2024-2025'
  match_date    DATE NOT NULL,          -- yyyy-mm-dd
  status        TEXT NOT NULL,          -- 'played'|'scheduled'
  home_team_id  TEXT NOT NULL REFERENCES team(team_id),
  away_team_id  TEXT NOT NULL REFERENCES team(team_id),
  home_goals    INTEGER,
  away_goals    INTEGER,
  attendance    INTEGER,
  venue         TEXT,
  source_url    TEXT,
  loaded_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- per-team stats for each match
CREATE TABLE IF NOT EXISTS team_match_stats (
  match_id        TEXT NOT NULL REFERENCES match(match_id),
  team_id         TEXT NOT NULL REFERENCES team(team_id),
  is_home         INTEGER NOT NULL CHECK (is_home IN (0,1)),
  xg              REAL,
  xga             REAL,
  shots           INTEGER,
  shots_on_target INTEGER,
  shots_on_target_pct REAL,
  corners         INTEGER,
  fouls           INTEGER,
  crosses         INTEGER,
  touches         INTEGER,
  tackles         INTEGER,
  interceptions   INTEGER,
  aerials_won     INTEGER,
  clearances      INTEGER,
  long_balls      INTEGER,
  passes          INTEGER,
  passes_completed INTEGER,
  pass_accuracy   REAL,
  saves           INTEGER,
  saves_total     INTEGER,
  save_pct       REAL,
  yellow          INTEGER,
  red             INTEGER,
  possession      REAL,
  PRIMARY KEY (match_id, team_id)
);


CREATE INDEX IF NOT EXISTS idx_match_date ON match(match_date);
CREATE INDEX IF NOT EXISTS idx_match_league_season ON match(league_id, season);
CREATE INDEX IF NOT EXISTS idx_tms_team_date ON team_match_stats(team_id);

