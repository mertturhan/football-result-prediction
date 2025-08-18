import hashlib
import re
from unidecode import unidecode

def formalize_team_name(name: str) -> str:
    name = unidecode(name).lower()
    name = re.sub(r'[^a-z0-9]+', '-', name).strip('-')
    return name[:25]      #keeping it short and readable

def produce_match_id(league_id, season, match_date, home_team_id, away_team_id):
    base = f"{league_id}|{season}|{match_date}|{home_team_id}|{away_team_id}"
    return hashlib.sha1(base.encode()).hexdigest()[:16]  #stable id with 16 digits


