"""BrokerWatch ingest: pull upstream data broker sources.

Each source produces a list of normalized records. Records are saved
per-source as JSON snapshots in data/sources/, preserving provenance.
"""

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

# Normalized record keys. Unknown fields fall back to "".
RECORD_KEYS = (
    "name", "url", "email", "address", "phone",
    "registration_id", "registered_date",
    "opt_out_url", "notes",
)


# ---------------------------------------------------------------- adapters

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
    """California AG data broker registry (mandatory self-registration)."""

    slug = "ccpa_registry"
    label = "California AG Data Broker Registry"

    # NOTE: verify this endpoint — the Delete Act portal may have replaced
    # the legacy download. Point this at the direct CSV/JSON download link.
    DOWNLOAD_URL = "https://oag.ca.gov/data-brokers"

    # Column names seen historically in the registry export, lowercase.
    COLUMN_MAP = {
        "name": ["data broker name", "business name", "name"],
        "url": ["data broker website", "website", "business website"],
        "email": ["email address", "email"],
        "address": ["business mailing address", "mailing address", "address"],
        "phone": ["phone number", "phone"],
        "registration_id": [
            "data broker business registration id",
            "prior registration number",
            "registration id",
        ],
        "registered_date": ["date registered", "registration date"],
    }

    def fetch(self) -> list[dict]:
        print(f"[{self.slug}] fetching {self.DOWNLOAD_URL}")
        resp = httpx.get(
            self.DOWNLOAD_URL,
            follow_redirects=True,
            timeout=60,
            headers={"User-Agent": "BrokerWatch/0.1 (open data project)"},
        )
        resp.raise_for_status()

        content_type = resp.headers.get("content-type", "")
        if "html" in content_type:
            # We hit a page, not a file — the endpoint needs updating.
            print(
                f"[{self.slug}] WARNING: received HTML, not a data file.\n"
                "    The download URL is stale or points at a portal page.\n"
                "    Open it in a browser and copy the direct CSV/JSON link.",
                file=sys.stderr,
            )
            return []

        reader = csv.DictReader(io.StringIO(resp.text))
        # Normalize headers to lowercase for flexible matching.
        lower = {h.lower().strip(): h for h in reader.fieldnames or []}
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
            records.append(self.make_record(
                source=self.slug,
                hq_country="US",
                jurisdiction="US-CA",
                **vals,
            ))
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
            print(f"[{self.slug}] no seed file at {self.filepath}, skipping")
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


# ---------------------------------------------------------------- pipeline

def build_sources() -> list[BaseSource]:
    return [
        CaliforniaCcpaRegistry(),
        CuratedListSource("canada_federal",
                          "Canada (federal, curated)", "CA", "CA-FED"),
        CuratedListSource("quebec",
                          "Quebec (curated)", "CA", "CA-QC"),
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
