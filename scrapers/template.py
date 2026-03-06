import requests
from bs4 import BeautifulSoup
from utils import generate_id


def fetch(url):
    results = []

    try:
        response = requests.get(url, timeout=10)
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return results

    soup = BeautifulSoup(response.text, "html.parser")

    # TEMP: Vi hämtar alla länkar bara för att testa flödet
    for link_tag in soup.find_all("a"):
        link = link_tag.get("href")

        if not link:
            continue

        title = link_tag.get_text(strip=True)

        if not title:
            continue

        results.append({
            "id": generate_id(link),
            "title": title,
            "company": "TEMPLATE",
            "location": "",
            "published": "",
            "url": link
        })

    return results
