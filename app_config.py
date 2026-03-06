import os

def env_bool(key: str, default: bool) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}

OUTPUTS = {
    "excel": env_bool("EXPORT_EXCEL", True),
    "web": env_bool("EXPORT_WEB", False),
}

EXPORT_DIR = os.path.join(os.path.dirname(__file__), "exports")

# Filter is opt-in via env in main.py now; keep this empty (or remove entirely)
LOCATION_FILTER = ""

SCRAPERS = [
    {"module": "biolit", "url": "https://biolit.se/konsultuppdrag/", "timeout_s": 300},
    {"module": "bohmans", "url": "https://www.bohmans.com/aktuellauppdrag/", "timeout_s": 300},
    {"module": "upgraded", "url": "https://upgraded.se/ort/stockholm/", "timeout_s": 300},
    {"module": "tingent", "url": "https://tingent.se/jobs", "timeout_s": 300},
    {"module": "headagent", "url": "https://www.headagent.se/uppdrag/", "timeout_s": 300},
    {"module": "asociety", "url": "https://www.asocietygroup.com/sv/uppdrag", "timeout_s": 300},
    {"module": "digitalenta", "url": "https://karriar.digitalenta.se/#jobs", "timeout_s": 300},
    {"module": "teksystems", "url": "https://careers.teksystems.com/gb/en", "timeout_s": 300},
    {"module": "verama", "url": "https://app.verama.com/en/job-requests?page=0&size=20&sortConfig=%5B%7B%22sortBy%22%3A%22firstDayOfApplications%22%2C%22order%22%3A%22DESC%22%7D%5D&filtersConfig=%7B%22location%22%3A%7B%22id%22%3Anull%2C%22signature%22%3A%22%22%2C%22city%22%3Anull%2C%22country%22%3A%22Sweden%22%2C%22name%22%3A%22Sweden%22%2C%22locationId%22%3A%22here%3Acm%3Anamedplace%3A20298368%22%2C%22countryCode%22%3A%22SWE%22%2C%22suggestedPhoneCode%22%3A%22SE%22%7D%2C%22remote%22%3A%5B%5D%2C%22query%22%3A%22%22%2C%22skillRoleCategories%22%3A%5B%5D%2C%22frequency%22%3A%22DAILY%22%2C%22radius%22%3A0%2C%22dedicated%22%3Afalse%2C%22originIds%22%3A%5B%5D%2C%22favouritesOnly%22%3Afalse%2C%22recommendedOnly%22%3Afalse%2C%22languages%22%3A%5B%5D%2C%22level%22%3A%5B%5D%2C%22skillIds%22%3A%5B%5D%2C%22skills%22%3A%5B%5D%7D", "timeout_s": 300},
    {"module": "enkl", "url": "https://enkl.se/lediga-uppdrag/", "timeout_s": 300},
    {"module": "emagine", "url": "https://portal.emagine.org/jobs/all", "engine": "browser", "timeout_s": 300},
    {"module": "nikita", "url": "https://www.nikita.se/lediga-uppdrag/","engine": "browser", "timeout_s": 300},
    {"module": "qbemanning", "url": "https://careers.qbemanning.se/jobs?split_view=true&query=&department=IT+%2F+Teknik", "timeout_s": 300},
    {"module": "wiseit", "url": "https://www.wise.se/specialistomraden/it/lediga-jobb/", "timeout_s": 300},
    {"module": "adadigital", "url": "https://www.adadigital.se/lediga-it-jobb/?", "timeout_s": 300},
]