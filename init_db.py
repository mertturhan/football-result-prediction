from pathlib import Path
import sqlite3

DB_DIR = Path("data/db")
SCHEMA_SQL = Path("sql/01_schema.sql").read_text()

DB_DIR.mkdir(parents=True, exist_ok=True)

if __name__ == "__main__":
    for name in ["men", "women"]:
        db_path = DB_DIR / f"{name}.sqlite"
        with sqlite3.connect(db_path) as con:
            con.execute("PRAGMA foreign_keys = ON;")
            con.executescript(SCHEMA_SQL)
        print(f"Schema created at {db_path.resolve()}")
