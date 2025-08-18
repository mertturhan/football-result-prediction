from pathlib import Path
import sqlite3

DB_PATH = Path("data/db/football.sqlite")
SCHEMA_SQL = Path("sql/01_schema.sql").read_text()

DB_PATH.parent.mkdir(parents=True, exist_ok=True)

if __name__ == "__main__":
    with sqlite3.connect(DB_PATH) as con:
        con.execute("PRAGMA foreign_keys = ON;")
        con.executescript(SCHEMA_SQL)
    print(f" Schema created at {DB_PATH.resolve()}")
