from pathlib import Path
import requests
from bs4 import BeautifulSoup
from sqlalchemy import text
from utils import league_mapping, get_closest_league, get_season_links, get_scores_and_fixtures_url
from src.db import get_engine, upsert_league, upsert_team
from src.ids import formalize_team_name, produce_match_id

START_SEASON_YEAR = 2010


def parse_fixtures_table(fixtures_url: str):
    """Return a list of match dicts from a Scores & Fixtures page."""
    resp = requests.get(fixtures_url)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table")
    fixtures = []
    if not table or not table.tbody:
        return fixtures
    for row in table.tbody.find_all("tr"):
        if row.get("class") and "spacer" in row.get("class"):
            continue
        date_cell = row.find("th", {"data-stat": "date"})
        if not date_cell or not date_cell.text.strip():
            continue
        match_date = date_cell.text.strip()
        home = row.find("td", {"data-stat": "home_team"}).text.strip()
        away = row.find("td", {"data-stat": "away_team"}).text.strip()
        home_g = row.find("td", {"data-stat": "home_score"}).text.strip()
        away_g = row.find("td", {"data-stat": "away_score"}).text.strip()
        report = row.find("td", {"data-stat": "match_report"})
        report_url = None
        if report and report.find("a"):
            report_url = "https://fbref.com" + report.find("a")["href"]
        fixtures.append({
            "date": match_date,
            "home": home,
            "away": away,
            "home_g": int(home_g) if home_g else None,
            "away_g": int(away_g) if away_g else None,
            "url": report_url,
        })
    return fixtures


def scrape_league(league_name: str, gender: str) -> None:
    gender_full = "Men" if gender.upper() == "M" else "Women"
    cache_root = Path("data/cache") / gender_full
    cache_root.mkdir(parents=True, exist_ok=True)
    leagues_cache = cache_root / "league_links.json"

    closest, info = get_closest_league(league_name, str(leagues_cache), gender)
    if not closest:
        return
    league_alias = league_mapping.get(closest, closest)
    league_dir = cache_root / league_alias
    league_dir.mkdir(parents=True, exist_ok=True)
    seasons_cache = league_dir / "season_links.json"
    seasons = get_season_links(str(seasons_cache), info["url"])

    engine = get_engine(gender_full.lower())
    with engine.begin() as conn:
        upsert_league(conn, league_alias, closest)
        for season_name, season_url in seasons.items():
            start_year = int(season_name.split("-")[0])
            if start_year < START_SEASON_YEAR:
                continue
            fixtures_url = get_scores_and_fixtures_url(season_url)
            if not fixtures_url:
                continue
            fixtures = parse_fixtures_table(fixtures_url)
            for f in fixtures:
                home_id = formalize_team_name(f["home"])
                away_id = formalize_team_name(f["away"])
                upsert_team(conn, home_id, f["home"])
                upsert_team(conn, away_id, f["away"])
                match_id = produce_match_id(
                    league_alias,
                    season_name,
                    f["date"],
                    home_id,
                    away_id,
                )
                status = "played" if f["home_g"] is not None else "scheduled"
                conn.execute(
                    text(
                        """
                        INSERT INTO match (
                            match_id, league_id, season, match_date, status,
                            home_team_id, away_team_id, home_goals, away_goals, source_url
                        ) VALUES (
                            :match_id, :league_id, :season, :match_date, :status,
                            :home_team_id, :away_team_id, :home_goals, :away_goals, :source_url
                        )
                        ON CONFLICT(match_id) DO NOTHING
                        """
                    ),
                    {
                        "match_id": match_id,
                        "league_id": league_alias,
                        "season": season_name,
                        "match_date": f["date"],
                        "status": status,
                        "home_team_id": home_id,
                        "away_team_id": away_id,
                        "home_goals": f["home_g"],
                        "away_goals": f["away_g"],
                        "source_url": f["url"],
                    },
                )
