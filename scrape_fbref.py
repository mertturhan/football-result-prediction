from pathlib import Path
import argparse
import logging
from utils import league_mapping, get_closest_league, get_season_links, get_scores_and_fixtures_url, get_league_links, \
    create_driver, rate_limited_get, get_match_links
from src.db import get_engine, upsert_league, upsert_team
from src.ids import formalize_team_name, produce_match_id
from sqlalchemy import text
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import re




STAT_KEYS = [
    "xg",
    "xga",
    "shots",
    "shots_on_target",
    "shots_on_target_pct",
    "corners",
    "fouls",
    "yellow",
    "red",
    "possession",
    "crosses",
    "touches",
    "tackles",
    "interceptions",
    "aerials_won",
    "clearances",
    "long_balls",
    "passes",
    "passes_completed",
    "pass_accuracy",
    "saves",
    "saves_total",
    "save_pct",
]

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

START_SEASON_YEAR = 2010


def parse_fixtures_table(fixtures_url: str):
    # Return a list of match dicts from a Scores & Fixtures page.
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
                        "//table[contains(@id,'sched')][.//th[@data-stat='home_team']]",
                    )
                )
            )
        except TimeoutException:
            logger.warning("No fixtures table found at %s", fixtures_url)
            return fixtures
        rows = table.find_elements(
            By.XPATH,
            ".//tbody/tr[not(contains(@class,'spacer')) and not(contains(@class,'thead'))]",
        )
        for row in rows:
            try:
                date_cell = row.find_element(By.XPATH, "./*[@data-stat='date']")
            except NoSuchElementException:
                continue
            match_date = date_cell.text.strip()
            if not match_date:
                continue
            home = row.find_element(By.XPATH, "./*[@data-stat='home_team']").text.strip()
            away = row.find_element(By.XPATH, "./*[@data-stat='away_team']").text.strip()
            try:
                score_text = row.find_element(By.XPATH, "./*[@data-stat='score']").text.strip()
                if score_text:
                    home_g_str, away_g_str = re.split(r"[-–—]", score_text)  # handles hyphen, en dash, em dash
                    home_g = int(home_g_str)
                    away_g = int(away_g_str)
                else:
                    home_g = away_g = None
            except NoSuchElementException:
                home_g = away_g = None
            report_links = row.find_elements(
                By.XPATH, "./*[@data-stat='match_report']//a"
            )
            report_url = (
                report_links[0].get_attribute("href") if report_links else None
            )
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


def _parse_percent(text: str) -> float | None:
    match = re.search(r"(\d+(?:\.\d+)?)", text)
    return float(match.group(1)) if match else None


def _parse_ratio(text: str):
    nums = re.findall(r"\d+", text)
    made = int(nums[0]) if len(nums) > 0 else None
    total = int(nums[1]) if len(nums) > 1 else None
    pct = float(nums[2]) if len(nums) > 2 else (
        (made / total * 100) if made is not None and total else None
    )
    return made, total, pct


def _parse_number(text: str) -> int | None:
    match = re.search(r"\d+", text)
    return int(match.group(0)) if match else None


def parse_match_report(report_url: str):
    """Return per-team stats from a match report page."""
    stats = {"home": {}, "away": {}}
    driver = create_driver()
    try:
        rate_limited_get(driver, report_url)
        # top stats table
        try:
            table = driver.find_element(By.XPATH, "//div[@id='team_stats']//table")
            rows = table.find_elements(By.XPATH, ".//tr")
            i = 0
            while i < len(rows):
                row = rows[i]
                ths = row.find_elements(By.TAG_NAME, "th")
                tds = row.find_elements(By.TAG_NAME, "td")
                label = None
                cells = None
                if len(ths) == 1 and len(tds) == 0 and i + 1 < len(rows):
                    # label row followed by data row
                    label = ths[0].text.strip().lower()
                    cells = rows[i + 1].find_elements(By.TAG_NAME, "td")
                    i += 2
                elif len(ths) == 1 and len(tds) >= 2:
                    # label and data in same row
                    label = ths[0].text.strip().lower()
                    cells = tds
                    i += 1
                else:
                    i += 1
                    continue
                if len(cells) < 2:
                    continue
                home_txt = cells[0].text.strip()
                away_txt = cells[-1].text.strip()
                logger.debug("Row '%s': home='%s' away='%s'", label, home_txt, away_txt)
                if label == "possession":
                    stats["home"]["possession"] = _parse_percent(home_txt)
                    stats["away"]["possession"] = _parse_percent(away_txt)
                elif label == "passing accuracy":
                    h_made, h_tot, h_pct = _parse_ratio(home_txt)
                    a_made, a_tot, a_pct = _parse_ratio(away_txt)
                    stats["home"].update(
                        {
                            "passes_completed": h_made,
                            "passes": h_tot,
                            "pass_accuracy": h_pct,
                        }
                    )
                    stats["away"].update(
                        {
                            "passes_completed": a_made,
                            "passes": a_tot,
                            "pass_accuracy": a_pct,
                        }
                    )
                elif label == "shots on target":
                    h_made, h_tot, h_pct = _parse_ratio(home_txt)
                    a_made, a_tot, a_pct = _parse_ratio(away_txt)
                    stats["home"].update(
                        {
                            "shots_on_target": h_made,
                            "shots": h_tot,
                            "shots_on_target_pct": h_pct,
                        }
                    )
                    stats["away"].update(
                        {
                            "shots_on_target": a_made,
                            "shots": a_tot,
                            "shots_on_target_pct": a_pct,
                        }
                    )
                elif label == "saves":
                    h_made, h_tot, h_pct = _parse_ratio(home_txt)
                    a_made, a_tot, a_pct = _parse_ratio(away_txt)
                    stats["home"].update(
                        {
                            "saves": h_made,
                            "saves_total": h_tot,
                            "save_pct": h_pct,
                        }
                    )
                    stats["away"].update(
                        {
                            "saves": a_made,
                            "saves_total": a_tot,
                            "save_pct": a_pct,
                        }
                    )
                elif label == "cards":
                    stats["home"]["yellow"] = len(
                        cells[0].find_elements(By.CSS_SELECTOR, ".yellow_card")
                    ) + len(
                        cells[0].find_elements(By.CSS_SELECTOR, ".yellow_red_card")
                    )
                    stats["home"]["red"] = len(
                        cells[0].find_elements(By.CSS_SELECTOR, ".red_card")
                    ) + len(
                        cells[0].find_elements(By.CSS_SELECTOR, ".yellow_red_card")
                    )
                    stats["away"]["yellow"] = len(
                        cells[-1].find_elements(By.CSS_SELECTOR, ".yellow_card")
                    ) + len(
                        cells[-1].find_elements(By.CSS_SELECTOR, ".yellow_red_card")
                    )
                    stats["away"]["red"] = len(
                        cells[-1].find_elements(By.CSS_SELECTOR, ".red_card")
                    ) + len(
                        cells[-1].find_elements(By.CSS_SELECTOR, ".yellow_red_card")
                    )
                    logger.debug(
                        "Parsed cards: home_yellow=%s home_red=%s away_yellow=%s away_red=%s",
                        stats["home"].get("yellow"),
                        stats["home"].get("red"),
                        stats["away"].get("yellow"),
                        stats["away"].get("red"),
                    )
        except NoSuchElementException:
            logger.warning("team_stats table not found on %s", report_url)
            # extra stats table
        try:
            extra = driver.find_element(By.XPATH, "//div[@id='team_stats_extra']")
            rows = extra.find_elements(By.XPATH, ".//tr")
            label_map = {
                "fouls": "fouls",
                "corners": "corners",
                "crosses": "crosses",
                "touches": "touches",
                "tackles": "tackles",
                "interceptions": "interceptions",
                "aerials won": "aerials_won",
                "clearances": "clearances",
                "long balls": "long_balls",
                "xg": "xg",
            }
            for row in rows:
                try:
                    label_el = row.find_elements(By.TAG_NAME, "th")
                    if not label_el:
                        continue
                    label = label_el[0].text.strip().lower()
                    if label not in label_map:
                        continue
                        cells = row.find_elements(By.TAG_NAME, "td")
                        if len(cells) < 2:
                            continue
                        home_txt = cells[0].text.strip()
                        away_txt = cells[-1].text.strip()
                        key = label_map[label]
                        if key == "xg":
                            stats["home"][key] = _parse_percent(home_txt)
                            stats["away"][key] = _parse_percent(away_txt)
                        else:
                            stats["home"][key] = _parse_number(home_txt)
                            stats["away"][key] = _parse_number(away_txt)
                        logger.debug(
                            "Parsed %s: home=%s away=%s",
                            label,
                            stats["home"].get(key),
                            stats["away"].get(key),
                        )
                except Exception:
                    continue
        except NoSuchElementException:
            logger.warning("team_stats_extra table not found on %s", report_url)
    finally:
        driver.quit()
    print(stats)
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
            season_dir = league_dir / season_name
            season_dir.mkdir(parents=True, exist_ok=True)
            matches_cache = season_dir / "match_links.json"
            fixtures = get_match_links(str(matches_cache), fixtures_url)
            logger.info(
                "Processing %d fixtures for season %s", len(fixtures), season_name
            )
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
                if f["url"]:
                    match_stats = parse_match_report(f["url"])
                    logger.debug(
                        "Scraped stats for %s vs %s: %s",
                        f["home"],
                        f["away"],
                        match_stats,
                    )
                    match_stats["home"]["xga"] = match_stats["away"].get("xg")
                    match_stats["away"]["xga"] = match_stats["home"].get("xg")
                    for is_home, team_id, side in [
                        (1, home_id, "home"),
                        (0, away_id, "away"),
                    ]:
                        stats = {k: match_stats.get(side, {}).get(k) for k in STAT_KEYS}
                        missing = {
                            k for k in STAT_KEYS if match_stats.get(side, {}).get(k) is None
                        }
                        if missing:
                            logger.debug(
                                "Missing stats %s for %s from %s",
                                ", ".join(sorted(missing)),
                                side,
                                f["url"],
                            )
                        stats.update(
                            {
                                "match_id": match_id,
                                "team_id": team_id,
                                "is_home": is_home,
                            }
                        )
                        conn.execute(
                            text(
                                """
                                INSERT INTO team_match_stats (
                                    match_id, team_id, is_home,
                                    xg, xga, shots, shots_on_target, shots_on_target_pct, corners, fouls,
                                    yellow, red, possession, crosses, touches, tackles, interceptions,
                                    aerials_won, clearances, long_balls, passes, passes_completed,
                                    pass_accuracy, saves, saves_total, save_pct
                                ) VALUES (
                                    :match_id, :team_id, :is_home,
                                    :xg, :xga, :shots, :shots_on_target, :shots_on_target_pct, :corners, :fouls,
                                    :yellow, :red, :possession, :crosses, :touches, :tackles, :interceptions,
                                    :aerials_won, :clearances, :long_balls, :passes, :passes_completed,
                                    :pass_accuracy, :saves, :saves_total, :save_pct
                                )
                                ON CONFLICT(match_id, team_id) DO UPDATE SET
                                    xg=excluded.xg,
                                    xga=excluded.xga,
                                    shots=excluded.shots,
                                    shots_on_target=excluded.shots_on_target,
                                    shots_on_target_pct=excluded.shots_on_target_pct,
                                    corners=excluded.corners,
                                    fouls=excluded.fouls,
                                    yellow=excluded.yellow,
                                    red=excluded.red,
                                    possession=excluded.possession,
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
                                    save_pct=excluded.save_pct
                                """
                            ),
                            stats,
                        )


def main(debug: bool = True):
    if debug:
        logging.getLogger().setLevel(logging.DEBUG)
    cache_root = Path("data/cache") / "Men"
    cache_root.mkdir(parents=True, exist_ok=True)
    leagues_cache = cache_root / "league_links.json"
    men_leagues, _ = get_league_links(str(leagues_cache))
    for league_name in league_mapping:
        if league_name in men_leagues:
            scrape_league(league_name, "M")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--debug", action="store_true", help="Enable debug logging"
    )
    args = parser.parse_args()
    main(debug=args.debug)





#//*[@id="all_sched"]