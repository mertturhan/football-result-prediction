import json
from pathlib import Path
from functools import lru_cache
from rapidfuzz import process
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException


def create_driver():
    opts= Options()
    opts.add_argument("--headless")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0 Safari/537.36"
    )
    return webdriver.Chrome(options=opts)


# mapping official fbref competition names to short aliases
league_mapping = {
    "2. Fußball-Bundesliga": "2. Bundesliga",
    "A-League Men": "A-League",
    "A-League Women": "A-League",
    "Allsvenskan": "Allsvenskan",
    "Austrian Football Bundesliga": "Bundesliga",
    "Belgian Pro League": "Pro League A",
    "Belgian Women's Super League": "Belgian WSL",
    "Campeonato Brasileiro Série A": "Série A",
    "Campeonato Brasileiro Série B": "Série B",
    "Canadian Premier League": "CanPL",
    "Categoría Primera A": "Primera A",
    "Challenger Pro League": "Pro League B",
    "Chilean Primera División": "Primera División",
    "Chinese Football Association Super League": "Super League",
    "CONCACAF Champions Cup": "CONCACAF CL",
    "Copa Libertadores": "Libertadores",
    "Copa Sudamericana": "Sudamericana",
    "Croatian Football League": "HNL",
    "Czech First League": "Czech First League",
    "Danish Superliga": "Danish Superliga",
    "Danish Women's League": "Kvindeligaen",
    "División de Fútbol Profesional": "Primera División",
    "EFL Championship": "Championship",
    "Ekstraklasa": "Ekstraklasa",
    "Eliteserien": "Eliteserien",
    "Eredivisie": "Eredivisie",
    "Eredivisie Vrouwen": "Eredivisie",
    "Eerste Divisie": "Eerste Divisie",
    "FA Women's Super League": "WSL",
    "FIFA Club World Cup": "Club WC",
    "First Professional Football League": "First League",
    "Frauen-Bundesliga": "Bundesliga",
    "Fußball-Bundesliga": "Bundesliga",
    "I-League": "I-League",
    "Indian Super League": "Super League",
    "J1 League": "J1 League",
    "J2 League": "J2 League",
    "K League 1": "K League",
    "La Liga": "La Liga",
    "Liga 1 de Fútbol Profesional": "Liga 1",
    "Liga F": "Liga F",
    "Liga I": "Liga I",
    "Liga MX": "Liga MX",
    "Liga Profesional de Fútbol Argentina": "Liga Argentina",
    "Liga Profesional Ecuador": "Serie A",
    "Liga FUTVE": "Liga FUTVE",
    "Ligue 1": "Ligue 1",
    "Ligue 2": "Ligue 2",
    "Major League Soccer": "MLS",
    "National Women's Soccer League": "NWSL",
    "Nemzeti Bajnokság I": "NB I",
    "North American Soccer League": "NASL",
    "ÖFB Frauen-Bundesliga": "ÖFB Frauenliga",
    "Paraguayan Primera División": "Primera Div",
    "Persian Gulf Pro League": "Pro League",
    "Premier League": "Premier League",
    "Première Ligue": "D1 Fém",
    "Primeira Liga": "Primeira Liga",
    "Russian Premier League": "Premier League",
    "Saudi Professional League": "Saudi Professional League",
    "Scottish Championship": "Championship",
    "Scottish Premiership": "Premiership",
    "Serie A": "Serie A",
    "Serie B": "Serie B",
    "South African Premier Division": "Premier Division",
    "Spanish Segunda División": "La Liga 2",
    "Superettan": "Superettan",
    "Super League Greece": "Super League",
    "Swiss Super League": "Super Lg",
    "Swiss Women's Super League": "Swiss WSL",
    "Süper Lig": "Süper Lig",
    "Toppserien": "Toppserien",
    "Ukrainian Premier League": "Premier League",
    "UEFA Champions League": "UCL",
    "UEFA Europa Conference League": "UECL",
    "UEFA Europa League": "UEL",
    "UEFA Women's Champions League": "UWCL",
    "Uruguayan Primera División": "Uruguayan Primera División",
    "USL Championship": "USL Champ",
    "USL First Division": "USL D-1",
    "USSF Division 2 Professional League": "D2 Pro League",
    "Veikkausliiga": "Veikkausliiga",
    "Venezuelan Primera División": "Liga FUTVE",
    "Women Empowerment League": "WE League"
}


def load_cache(cache_file: Path) -> dict:
    if cache_file.exists():
        return json.loads(cache_file.read_text())
    return {}


def save_cache(data: dict, cache_file: Path) -> None:
    cache_file.write_text(json.dumps(data))


def scrape_league_links():
    url = "https://fbref.com/en/comps/"
    driver = create_driver()
    driver.get(url)

    men_league_dict, women_league_dict = {}, {}  # return 2 empty dictionaries

    for table_id in ['comps_1_fa_club_league_senior', 'comps_2_fa_club_league_senior']:
        try:
            table = driver.find_element(By.ID, table_id)
        except NoSuchElementException:
            continue

        rows = table.find_element(By.CSS_SELECTOR, "tbody tr")
        for row in rows:
        cols = row.find_elements(By.TAG_NAME, "td")
        headers = row.find_elements(By.TAG_NAME, "th")
        if not cols or not headers:
            continue
        gender = cols[0].text.strip()

        try:
            link_tag = headers[0].find_element(By.TAG_NAME, "a")
        except NoSuchElementException:
            continue

        league_name = link_tag.text.strip()
        league_url = link_tag.get_attribute("href")
        target = men_league_dict if gender == 'M' else women_league_dict
        target[league_name] = {"url": league_url, "gender": gender}

    driver.quit()
    return men_league_dict, women_league_dict


@lru_cache(maxsize=32)  # Caches the result in memory for 32 different league scrapes
def get_league_links(cache_file: str):
    """
    Function to get league links, using caching to avoid redundant scraping
    """
    path = Path(cache_file)
    data = load_cache(path)
    if not data:  # if cache is empty, scrape league URLs
        data = scrape_league_links()
        save_cache(data, path)
    return data


def get_closest_league(input_league: str, cache_file: str, gender: str):
    """
    Fuzzy matching to get the closest league name with the specified gender
    """
    men_dict, women_dict = get_league_links(cache_file)  # Fetch the league dictionaries
    league_dict = men_dict if gender.upper() == 'M' else women_dict  # Select the appropriate dictionary based on gender
    league_names = list(league_dict.keys())  # List of league names after filtering
    if not league_names:
        return None, None
    match = process.extractOne(input_league, league_names)  # Fuzzy match
    if match and match[1] > 80:  # Set a threshold for accuracy (80% in this case)
        name = match[0]
        return name, league_dict[name]
    return None, None


def scrape_season_links(league_url: str) -> dict:
    """
    function to scrape league links from fbref's main competitions page
    """
    driver = create_driver()
    driver.get(league_url)
    seasons_dict = {}
    try:
        table = driver.find_element(By.ID, 'seasons')
        rows = table.find_elements(By.CSS_SELECTOR, 'tbody th')
        for row in rows:
            try:
                link = row.find_element(By.TAG_NAME, 'a')
                seasons_dict[link.text.strip()] = link.get_attribute('href')
            except NoSuchElementException:
                continue
    except NoSuchElementException:
        pass
    finally:
        driver.quit()
    return seasons_dict


@lru_cache(maxsize=64)
def get_season_links(cache_file: str, league_url: str) -> dict:
    """
    function to get season links, using caching to avoid redundant scraping
    """
    path = Path(cache_file)
    data = load_cache(path)
    if not data:
        data = scrape_season_links(league_url)
        save_cache(data, path)
    return data


def get_scores_and_fixtures_url(competition_url: str):
    driver = create_driver()
    driver.get(competition_url)
    try:
        inner_nav = driver.find_element(By.ID, 'inner_nav')
        link = inner_nav.find_element(By.LINK_TEXT, "Scores & Fixtures")
        return link.get_attribute('href')
    except NoSuchElementException:
        return None
    finally:
        driver.quit()

