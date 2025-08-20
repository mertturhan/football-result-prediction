from pathlib import Path
import logging
from utils import league_mapping, get_closest_league, get_season_links, get_scores_and_fixtures_url, get_league_links, \
    create_driver, rate_limited_get
from src.db import get_engine, upsert_league, upsert_team
from src.ids import formalize_team_name, produce_match_id
from sqlalchemy import text
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import re

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
        try:
            table = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located(
                    (
                        By.XPATH,
                        "//table[contains(@id,'sched')][.//th[@data-stat='date']]",
                    )
                )
            )
        except TimeoutException:
            logger.warning("No fixtures table found at %s", fixtures_url)
            return fixtures
        rows = table.find_elements(
            By.XPATH, ".//tbody/tr[not(contains(@class,'spacer'))]"
        )
        for row in rows:
            try:
                date_cell = row.find_element(By.XPATH, ".//th[@data-stat='date']")
            except NoSuchElementException:
                continue
            match_date = date_cell.text.strip()
            if not match_date:
                continue
            home = row.find_element(By.XPATH, "./td[@data-stat='home_team']").text.strip()
            away = row.find_element(By.XPATH, "./td[@data-stat='away_team']").text.strip()
            home_g = row.find_element(By.XPATH, "./td[@data-stat='home_score']").text.strip()
            away_g = row.find_element(By.XPATH, "./td[@data-stat='away_score']").text.strip()
            report_links = row.find_elements(By.XPATH, "./td[@data-stat='match_report']//a")
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
    finally:
        driver.quit()
    logger.info("Parsed %d fixtures from %s", len(fixtures), fixtures_url)
    return fixtures


def parse_match_stats(match_url: str):
    """Scrape detailed team statistics from a match report page."""
    driver = create_driver()
    stats = {"home": {}, "away": {}}
    try:
        rate_limited_get(driver, match_url)

        def parse_pct(text: str):
            m = re.search(r"([0-9]+(?:\.[0-9]+)?)%", text)
            return float(m.group(1)) if m else None

        def parse_of_pct(text: str):
            nums = [int(n) for n in re.findall(r"\d+", text)]
            pct = parse_pct(text)
            if len(nums) >= 2:
                return nums[0], nums[1], pct
            elif len(nums) == 1:
                return nums[0], None, pct
            return None, None, pct

        def parse_int(text: str):
            m = re.search(r"\d+", text.replace(",", ""))
            return int(m.group(0)) if m else None

        def norm(name: str):
            mapping = {
                "fouls": "fouls",
                "corners": "corners",
                "crosses": "crosses",
                "touches": "touches",
                "tackles": "tackles",
                "interceptions": "interceptions",
                "aerials won": "aerials_won",
                "clearances": "clearances",
                "long balls": "long_balls",
            }
            return mapping.get(name)

        # xG from scorebox
        try:
            teams = driver.find_elements(By.CSS_SELECTOR, "div.scorebox div.team")
            if len(teams) >= 2:
                hxg_text = teams[0].find_element(
                    By.XPATH, ".//div[contains(@class,'score_xg')]").text
                axg_text = teams[1].find_element(
                    By.XPATH, ".//div[contains(@class,'score_xg')]").text
                home_xg = float(hxg_text) if hxg_text else None
                away_xg = float(axg_text) if axg_text else None
                stats["home"]["xg"] = home_xg
                stats["home"]["xga"] = away_xg
                stats["away"]["xg"] = away_xg
                stats["away"]["xga"] = home_xg
        except Exception:
            pass

        # Bars table
        try:
            team_stats = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.ID, "team_stats"))
            )
            for row in team_stats.find_elements(By.XPATH, ".//tbody/tr"):
                label = row.find_element(By.TAG_NAME, "th").text.strip().lower()
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) < 2:
                    continue
                home_cell, away_cell = cells[0], cells[-1]
                if label == "possession":
                    stats["home"]["possession"] = parse_pct(home_cell.text)
                    stats["away"]["possession"] = parse_pct(away_cell.text)
                elif label == "passing accuracy":
                    hc, ht, hp = parse_of_pct(home_cell.text)
                    ac, at, ap = parse_of_pct(away_cell.text)
                    stats["home"].update(
                        {"passes_completed": hc, "passes": ht, "pass_accuracy": hp}
                    )
                    stats["away"].update(
                        {"passes_completed": ac, "passes": at, "pass_accuracy": ap}
                    )
                elif label == "shots on target":
                    hsot, hs, hp = parse_of_pct(home_cell.text)
                    asot, ass, ap = parse_of_pct(away_cell.text)
                    stats["home"].update(
                        {
                            "shots_on_target": hsot,
                            "shots": hs,
                            "shots_on_target_pct": hp,
                        }
                    )
                    stats["away"].update(
                        {
                            "shots_on_target": asot,
                            "shots": ass,
                            "shots_on_target_pct": ap,
                        }
                    )
                elif label == "saves":
                    hs, ht, hp = parse_of_pct(home_cell.text)
                    asv, at, ap = parse_of_pct(away_cell.text)
                    stats["home"].update(
                        {"saves": hs, "saves_total": ht, "saves_pct": hp}
                    )
                    stats["away"].update(
                        {"saves": asv, "saves_total": at, "saves_pct": ap}
                    )
                elif label == "cards":
                    stats["home"]["yellow"] = len(
                        home_cell.find_elements(By.CLASS_NAME, "yellow_card")
                    )
                    stats["home"]["red"] = len(
                        home_cell.find_elements(By.CLASS_NAME, "red_card")
                    )
                    stats["away"]["yellow"] = len(
                        away_cell.find_elements(By.CLASS_NAME, "yellow_card")
                    )
                    stats["away"]["red"] = len(
                        away_cell.find_elements(By.CLASS_NAME, "red_card")
                    )
        except TimeoutException:
            pass

        # Extra stats tables
        try:
            extra = driver.find_element(By.ID, "team_stats_extra")
            tables = extra.find_elements(By.TAG_NAME, "table")
            if len(tables) >= 2:
                home_rows = tables[0].find_elements(By.XPATH, ".//tbody/tr")
                away_rows = tables[1].find_elements(By.XPATH, ".//tbody/tr")
                for row in home_rows:
                    name = row.find_element(By.TAG_NAME, "th").text.strip().lower()
                    key = norm(name)
                    if key:
                        stats["home"][key] = parse_int(
                            row.find_element(By.TAG_NAME, "td").text
                        )
                for row in away_rows:
                    name = row.find_element(By.TAG_NAME, "th").text.strip().lower()
                    key = norm(name)
                    if key:
                        stats["away"][key] = parse_int(
                            row.find_element(By.TAG_NAME, "td").text
                        )
        except NoSuchElementException:
            pass
    finally:
        driver.quit()
    return stats


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

                if status == "played" and f.get("url"):
                    team_stats = parse_match_stats(f["url"])
                    for is_home, team_id in ((1, home_id), (0, away_id)):
                        side = "home" if is_home else "away"
                        s = team_stats.get(side, {})
                        params = {
                            "match_id": match_id,
                            "team_id": team_id,
                            "is_home": is_home,
                            "xg": s.get("xg"),
                            "xga": s.get("xga"),
                            "shots": s.get("shots"),
                            "shots_on_target": s.get("shots_on_target"),
                            "shots_on_target_pct": s.get("shots_on_target_pct"),
                            "corners": s.get("corners"),
                            "fouls": s.get("fouls"),
                            "crosses": s.get("crosses"),
                            "touches": s.get("touches"),
                            "tackles": s.get("tackles"),
                            "interceptions": s.get("interceptions"),
                            "aerials_won": s.get("aerials_won"),
                            "clearances": s.get("clearances"),
                            "long_balls": s.get("long_balls"),
                            "passes": s.get("passes"),
                            "passes_completed": s.get("passes_completed"),
                            "pass_accuracy": s.get("pass_accuracy"),
                            "saves": s.get("saves"),
                            "saves_total": s.get("saves_total"),
                            "saves_pct": s.get("saves_pct"),
                            "yellow": s.get("yellow"),
                            "red": s.get("red"),
                            "possession": s.get("possession"),
                        }
                        conn.execute(
                            text(
                                """
                                INSERT INTO team_match_stats (
                                    match_id, team_id, is_home, xg, xga, shots, shots_on_target, shots_on_target_pct,
                                    corners, fouls, crosses, touches, tackles, interceptions, aerials_won, clearances,
                                    long_balls, passes, passes_completed, pass_accuracy, saves, saves_total, saves_pct,
                                    yellow, red, possession
                                ) VALUES (
                                    :match_id, :team_id, :is_home, :xg, :xga, :shots, :shots_on_target, :shots_on_target_pct,
                                    :corners, :fouls, :crosses, :touches, :tackles, :interceptions, :aerials_won, :clearances,
                                    :long_balls, :passes, :passes_completed, :pass_accuracy, :saves, :saves_total, :saves_pct,
                                    :yellow, :red, :possession
                                )
                                ON CONFLICT(match_id, team_id) DO UPDATE SET
                                    xg=excluded.xg,
                                    xga=excluded.xga,
                                    shots=excluded.shots,
                                    shots_on_target=excluded.shots_on_target,
                                    shots_on_target_pct=excluded.shots_on_target_pct,
                                    corners=excluded.corners,
                                    fouls=excluded.fouls,
                                    crosses=excluded.crosses,
                                    touches=excluded.touches,
                                    tackles=excluded.tackles,
                                    interceptions=excluded.interceptions,
                                    aerials_won=excluded.aerials_won,
                                    clearances=excluded.clearances,
                                    long_balls=excluded.long_balls,
                                    passes=excluded.passes,
                                    passes_completed=excluded.passes_completed,
                                    pass_accuracy=excluded.pass_accuracy,
                                    saves=excluded.saves,
                                    saves_total=excluded.saves_total,
                                    saves_pct=excluded.saves_pct,
                                    yellow=excluded.yellow,
                                    red=excluded.red,
                                    possession=excluded.possession
                                """
                            ),
                            params,
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


#//*[@id="all_sched"]