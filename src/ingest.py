"""BrokerWatch ingest: pull upstream data broker sources."""
from __future__ import annotations

import csv
import io
import json
import sys
from datetime import date
from pathlib import Path

import httpx
import yaml

ROOT = Path(__file__).resolve().parent.parent
SOURCES_DIR = ROOT / "data" / "sources"
SOURCES_DIR.mkdir(parents=True, exist_ok=True)
TODAY = str(date.today())

RECORD_KEYS = ("name", "url", "email", "address", "phone", "registration_id", "registered_date", "opt_out_url", "notes")


class BaseSource:
    slug = "base"
    label = "Base"

    def fetch(self) -> list[dict]:
        raise NotImplementedError

    @staticmethod
    def make_record(source: str, **kw) -> dict:
        rec = {k: str(kw.get(k, "")).strip() for k in RECORD_KEYS}
        rec.update({
            "hq_country": kw.get("hq_country", ""),
            "jurisdiction": kw.get("jurisdiction", source),
            "source": source,
            "first_seen": TODAY,
        })
        return rec

    def save(self, records: list[dict]) -> None:
        out = SOURCES_DIR / f"{self.slug}.json"
        out.write_text(json.dumps(records, indent=2, ensure_ascii=False))
        print(f"[{self.slug}] saved {len(records)} records -> {out}")


class CaliforniaCcpaRegistry(BaseSource):
    """California CPPA data broker registry (mandatory self-registration)."""

    slug = "ccpa_registry"
    label = "California Data Broker Registry"
    DOWNLOAD_URL = "https://cppa.ca.gov/data_broker_registry/registry.csv"

    COLUMN_MAP = {
        "name": ["data broker name", "business name"],
        "url": [
            "data broker primary website",
            "data broker's primary website",
            "data broker website",
        ],
        "email": [
            "data broker primary contact email address",
            "email address",
        ],
        "address": [
            "data broker primary street address",
            "business mailing address",
        ],
        "phone": [
            "data broker primary phone number [optional]",
            "data broker primary phone number",
            "phone number",
        ],
        "registration_id": [
            "data broker business registration id",
            "prior registration number",
        ],
        "registered_date": ["date registered"],
        "opt_out_url": [
            "data broker's primary website that contains details on how consumers can exercise their ca consumer privacy rights",
        ],
    }

    def fetch(self) -> list[dict]:
        print(f"[{self.slug}] fetching {self.DOWNLOAD_URL}")
        resp = httpx.get(
            self.DOWNLOAD_URL,
            follow_redirects=True,
            timeout=60,
            headers={"User-Agent": "BrokerWatch/0.1"},
        )
        resp.raise_for_status()

        if "html" in resp.headers.get("content-type", ""):
            print(f"[{self.slug}] WARNING: received HTML, not CSV", file=sys.stderr)
            return []

        csv_text = resp.content.decode("utf-8-sig", errors="replace")
        reader = csv.DictReader(io.StringIO(csv_text))

        lower = {
            h.replace("\u2019", "'").lower().strip().strip(":").strip(): h
            for h in reader.fieldnames or []
        }

        col_lookup = {}
        for canon, candidates in self.COLUMN_MAP.items():
            for cand in candidates:
                if cand in lower:
                    col_lookup[canon] = lower[cand]
                    break

        records = []
        for row in reader:
            vals = {canon: row.get(col, "") for canon, col in col_lookup.items()}
            if not vals.get("name"):
                continue
            records.append(
                self.make_record(
                    source=self.slug,
                    hq_country="US",
                    jurisdiction="US-CA",
                    **vals,
                )
            )
        return records


class CuratedListSource(BaseSource):
    """Curated seed lists maintained by hand (YAML) in data/sources/."""

    def __init__(self, slug: str, label: str, country: str, jurisdiction: str):
        self.slug = slug
        self.label = label
        self.country = country
        self.jurisdiction = jurisdiction
        self.filepath = SOURCES_DIR / f"{slug}.yaml"

    def fetch(self) -> list[dict]:
        if not self.filepath.exists():
            return []
        entries = yaml.safe_load(self.filepath.read_text()) or []
        return [
            self.make_record(
                source=f"manual:{self.slug}",
                hq_country=self.country,
                jurisdiction=self.jurisdiction,
                **entry,
            )
            for entry in entries
        ]


def build_sources() -> list[BaseSource]:
    return [
        CaliforniaCcpaRegistry(),
        CuratedListSource("canada_federal", "Canada federal", "CA", "CA-FED"),
        CuratedListSource("quebec", "Quebec", "CA", "CA-QC"),
    ]


def main() -> int:
    exit_code = 0
    for src in build_sources():
        try:
            records = src.fetch()
            src.save(records)
        except Exception as exc:
            print(f"[{src.slug}] FAILED: {exc}", file=sys.stderr)
            exit_code = 1
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
