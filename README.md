# BrokerWatch

A continuously updated, open database of data brokers, aggregated from
official registries and community-maintained lists.

## What It Does

BrokerWatch ingests data broker registries from multiple jurisdictions,
normalizes them into a unified schema, and publishes per-source JSON
snapshots — making it easy to track who the brokers are, what data they
hold, and how to opt out.

## Data Sources

| Source | Jurisdiction | Status |
|--------|--------------|--------|
| California CPPA Data Broker Registry (Delete Act) | US-CA | Automated ingestion (~600 records) |
| Canada federal (curated) | CA | Curated YAML seed list |
| Quebec (curated) | CA-QC | Curated YAML seed list |

**Planned sources:** 
BADBOOL (Big Ass Data Broker Opt-Out List),
GitLab's public data broker inventory
Privacy Rights Clearinghouse.

## Pipeline
Ingestion runs automatically via GitHub Actions (weekly, Mondays 09:00 UTC)
and commits updated snapshots to this repository. Manual runs: Actions tab
→ "Refresh data broker registries" → Run workflow.

## Record Schema

Each normalized record contains:

| Field | Description |
|-------|-------------|
| `name` | Broker / company name |
| `url` | Primary website |
| `email` | Primary privacy contact |
| `address` | Mailing address (street portion) |
| `phone` | Primary phone number |
| `opt_out_url` | Link for consumers to exercise deletion / opt-out rights |
| `hq_country` | Headquarter country |
| `jurisdiction` | Registry jurisdiction (e.g. `US-CA`, `CA-QC`) |
| `source` | Provenance (which source the record came from) |
| `first_seen` | Date BrokerWatch first ingested the record |

## Usage
```
```bash
# Install dependencies
pip install -r requirements.txt

# Run the ingestion pipeline
python -m src.ingest

# Query the results
jq '.[0]' data/sources/ccpa_registry.json
jq '[.[] | select(.opt_out_url != "")] | length' data/sources/ccpa_registry.json
