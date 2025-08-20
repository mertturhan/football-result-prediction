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

            # Backfill columns that might be missing from existing databases
            existing = {
                row[1] for row in con.execute("PRAGMA table_info(team_match_stats)")
            }
            required_cols = {
                "shots_on_target_pct": "REAL",
                "crosses": "INTEGER",
                "touches": "INTEGER",
                "tackles": "INTEGER",
                "interceptions": "INTEGER",
                "aerials_won": "INTEGER",
                "clearances": "INTEGER",
                "long_balls": "INTEGER",
                "passes": "INTEGER",
                "passes_completed": "INTEGER",
                "pass_accuracy": "REAL",
                "saves": "INTEGER",
                "saves_total": "INTEGER",
                "save_pct": "REAL",
            }
            for col, col_type in required_cols.items():
                if col not in existing:
                    con.execute(
                        f"ALTER TABLE team_match_stats ADD COLUMN {col} {col_type}"
                    )
        print(f"Schema created at {db_path.resolve()}")
