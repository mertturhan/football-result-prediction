CREATE TABLE IF NOT EXISTS league(
    league_id   TEXT PRIMARY KEY,   --  e.g. EPL
    name        TEXT NOT NULL,
    country     TEXT
);

CREATE TABLE IF NOT EXISTS team (
    team_id     TEXT PRIMARY KEY,   -- e.g. 'manchester-city'
    name        TEXT NOT NULL,
    country     TEXT
);

CREATE TABLE IF NOT EXISTS team_alias (
    alias       TEXT PRIMARY KEY,
    team_id     TEXT NOT NULL REFERENCES team(team_id)
);

CREATE TABLE IF NOT EXISTS match (
  match_id      TEXT PRIMARY KEY,  -- FBRef id or sha1 code
  league_id     TEXT NOT NULL REFERENCES league(league_id),
  season        TEXT NOT NULL,     -- '2024-2025'
  match_date    DATE NOT NULL,     -- 'YYYY-MM-DD'
  status        TEXT NOT NULL,     -- 'played'|'scheduled'
  home_team_id  TEXT NOT NULL REFERENCES team(team_id),
  away_team_id  TEXT NOT NULL REFERENCES team(team_id),
  home_goals    INTEGER,
  away_goals    INTEGER,
  source_url    TEXT,
  loaded_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS team_match_stat_kv (
  match_id   TEXT NOT NULL REFERENCES match(match_id),
  team_id    TEXT NOT NULL REFERENCES team(team_id),
  stat_key   TEXT NOT NULL,        -- e.g. 'xg', 'shots_on_target'
  stat_val   REAL,                 -- numeric; store text elsewhere if needed
  PRIMARY KEY (match_id, team_id, stat_key)
);

CREATE INDEX IF NOT EXISTS idx_match_league_season ON match(league_id, season);
CREATE INDEX IF NOT EXISTS idx_match_date ON match(match_date);
CREATE INDEX IF NOT EXISTS idx_kv_team ON team_match_stat_kv(team_id);