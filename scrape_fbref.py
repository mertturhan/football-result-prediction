from pathlib import Path
import logging
from utils import league_mapping, get_closest_league, get_season_links, get_scores_and_fixtures_url, get_league_links, \
    create_driver, rate_limited_get
from src.db import get_engine, upsert_league, upsert_team
from src.ids import formalize_team_name, produce_match_id
from sqlalchemy import text
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

START_SEASON_YEAR = 2010


def parse_fixtures_table(fixtures_url: str):
    """Return a list of match dicts from a Scores & Fixtures page."""
    logger.debug("Fetching fixtures from %s", fixtures_url)
    driver = create_driver()
    fixtures = []
    try:
        rate_limited_get(driver, fixtures_url)
        table = driver.find_element(By.TAG_NAME, "table")
        rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
        for row in rows:
            classes = row.get_attribute("class") or ""
            if "spacer" in classes:
                continue
            try:
                date_cell = row.find_element(By.CSS_SELECTOR, "th[data-stat='date']")
            except NoSuchElementException:
                continue
            match_date = date_cell.text.strip()
            if not match_date:
                continue
            home = row.find_element(By.CSS_SELECTOR, "td[data-stat='home_team']").text.strip()
            away = row.find_element(By.CSS_SELECTOR, "td[data-stat='away_team']").text.strip()
            home_g = row.find_element(By.CSS_SELECTOR, "td[data-stat='home_score']").text.strip()
            away_g = row.find_element(By.CSS_SELECTOR, "td[data-stat='away_score']").text.strip()
            report_links = row.find_elements(By.CSS_SELECTOR, "td[data-stat='match_report'] a")
            report_url = report_links[0].get_attribute("href") if report_links else None
            fixtures.append(
                {
                    "date": match_date,
                    "home": home,
                    "away": away,
                    "home_g": int(home_g) if home_g else None,
                    "away_g": int(away_g) if away_g else None,
                    "url": report_url,
                }
            )
    except NoSuchElementException:
        logger.warning("No fixtures table found at %s", fixtures_url)
    finally:
        driver.quit()
    logger.info("Parsed %d fixtures from %s", len(fixtures), fixtures_url)
    return fixtures


def scrape_league(league_name: str, gender: str) -> None:
    gender_full = "Men" if gender.upper() == "M" else "Women"
    logger.info("Scraping league %s for %s", league_name, gender_full)
    cache_root = Path("data/cache") / gender_full
    cache_root.mkdir(parents=True, exist_ok=True)
    leagues_cache = cache_root / "league_links.json"

    closest, info = get_closest_league(league_name, str(leagues_cache), gender)
    if not closest:
        logger.error("Could not find league %s for %s", league_name, gender_full)
        return
    print(f"Scraping league {closest} ({gender_full})...")
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
                logger.debug(
                    "Skipping season %s with start year %s", season_name, start_year
                )
                continue
            fixtures_url = get_scores_and_fixtures_url(season_url)
            if not fixtures_url:
                logger.warning("No fixtures URL found for season %s", season_name)
                continue
            fixtures = parse_fixtures_table(fixtures_url)
            logger.info("Processing %d fixtures for season %s", len(fixtures), season_name)
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


def main() -> None:
    cache_root = Path("data/cache") / "Men"
    cache_root.mkdir(parents=True, exist_ok=True)
    leagues_cache = cache_root / "league_links.json"
    men_leagues, _ = get_league_links(str(leagues_cache))
    for league_name in league_mapping:
        if league_name in men_leagues:
            scrape_league(league_name, "M")


if __name__ == "__main__":
    main()
