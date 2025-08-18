from pathlib import Path
from sqlalchemy import create_engine, text, event

def get_engine(gender: str = "men"):
    """Return a SQLite engine for the specified gender's database."""
    db_path = Path(f"data/db/{gender.lower()}.sqlite")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    engine = create_engine(f"sqlite:///{db_path}", future=True)

    @event.listens_for(engine, "connect")
    def _fk_on(dbapi_conn, _):
        dbapi_conn.execute("PRAGMA foreign_keys = ON;")

    return engine

def upsert_league(conn, league_id, name, country=None):
    """
    Function to insert a new row into the 'league' table. If a row with the same 'league_id' already exists, SQLite will
    ignore the insert instead of raising an error.
    """
    conn.execute(text("""
    INSERT INTO league (league_id, name, country)
    VALUES (:league_id, :name, :country)
    ON CONFLICT(league_id) DO NOTHING
    """), {"league_id": league_id, "name": name, "country": country})

def upsert_team(conn, team_id, name, country=None):
    """
    Similar to 'upsert_league' but if a 'team_id' already exists, SQLite will update the row with the new name and
    country
    """
    conn.execute(text("""
    INSERT INTO team (team_id, name, country)
    VALUES (:team_id, :name, :country)
    ON CONFLICT(team_id) DO UPDATE SET name=excluded.name, country=excluded.country
    """), {"team_id": team_id, "name": name, "country": country})
