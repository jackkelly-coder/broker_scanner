# Broker Scanner

Broker Scanner samlar in svenska IT-konsultuppdrag från flera mäklar- och konsultplattformar, normaliserar datan, deduplicerar på `(company, url)` och exporterar resultatet till CSV och XLSX.

## Funktioner

- Flera scrapers i samma körning
- Separat styrning för HTTP- och browser-baserade scrapers
- Subprocess-timeouts för robustare körningar
- Sweden-only-filter
- Frivilligt platsfilter via `LOCATION_FILTER`
- Kvalitetsfiltrering av skräpannonser
- SQLite-lagring med upsert och dedupe
- Export till CSV och XLSX

## Projektstruktur

- `main.py` – orchestrator
- `app_config.py` – scraper-konfiguration
- `config.py` – lokal runtime-config
- `database.py` – SQLite och upsert/dedupe
- `export.py` – CSV/XLSX-export
- `quality.py` – kvalitetsregler
- `geo.py` – Sverige- och platsklassning
- `scrapers/` – en scraper per källa

## Installation

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp config.example.py config.py