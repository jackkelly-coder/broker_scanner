import os
from dataclasses import dataclass
from typing import Literal

Engine = Literal["http", "browser"]


def env_bool(key: str, default: bool) -> bool:
    value = os.getenv(key)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass(frozen=True)
class ScraperConfig:
    module: str
    url: str
    timeout_s: int = 300
    engine: Engine = "http"


OUTPUTS = {
    "excel": env_bool("EXPORT_EXCEL", True),
    "web": env_bool("EXPORT_WEB", False),
}

EXPORT_DIR = os.path.join(os.path.dirname(__file__), "exports")

SCRAPERS = [
    ScraperConfig("biolit", "https://biolit.se/konsultuppdrag/"),
    ScraperConfig("bohmans", "https://www.bohmans.com/aktuellauppdrag/"),
    ScraperConfig("upgraded", "https://upgraded.se/ort/stockholm/"),
    ScraperConfig("tingent", "https://tingent.se/jobs"),
    ScraperConfig("headagent", "https://www.headagent.se/uppdrag/"),
    ScraperConfig("asociety", "https://www.asocietygroup.com/sv/uppdrag"),
    ScraperConfig("digitalenta", "https://karriar.digitalenta.se/#jobs"),
    ScraperConfig("teksystems", "https://careers.teksystems.com/gb/en"),
    ScraperConfig(
        "verama",
        "https://app.verama.com/en/job-requests?page=0&size=20&sortConfig=%5B%7B%22sortBy%22%3A%22firstDayOfApplications%22%2C%22order%22%3A%22DESC%22%7D%5D&filtersConfig=%7B%22location%22%3A%7B%22id%22%3Anull%2C%22signature%22%3A%22%22%2C%22city%22%3Anull%2C%22country%22%3A%22Sweden%22%2C%22name%22%3A%22Sweden%22%2C%22locationId%22%3A%22here%3Acm%3Anamedplace%3A20298368%22%2C%22countryCode%22%3A%22SWE%22%2C%22suggestedPhoneCode%22%3A%22SE%22%7D%2C%22remote%22%3A%5B%5D%2C%22query%22%3A%22%22%2C%22skillRoleCategories%22%3A%5B%5D%2C%22frequency%22%3A%22DAILY%22%2C%22radius%22%3A0%2C%22dedicated%22%3Afalse%2C%22originIds%22%3A%5B%5D%2C%22favouritesOnly%22%3Afalse%2C%22recommendedOnly%22%3Afalse%2C%22languages%22%3A%5B%5D%2C%22level%22%3A%5B%5D%2C%22skillIds%22%3A%5B%5D%2C%22skills%22%3A%5B%5D%7D",
    ),
    ScraperConfig("enkl", "https://enkl.se/lediga-uppdrag/"),
    ScraperConfig("emagine", "https://portal.emagine.org/jobs/all", engine="browser"),
    ScraperConfig("nikita", "https://www.nikita.se/lediga-uppdrag/", engine="browser"),
    ScraperConfig(
        "qbemanning",
        "https://careers.qbemanning.se/jobs?split_view=true&query=&department=IT+%2F+Teknik",
    ),
    ScraperConfig("wiseit", "https://www.wise.se/specialistomraden/it/lediga-jobb/"),
    ScraperConfig("adadigital", "https://www.adadigital.se/lediga-it-jobb/?"),
]