# strava-analytics

ELT pipeline for personal Strava activity data, built on a medallion architecture in Google Cloud.

## Architecture

| Layer | Where | What |
|---|---|---|
| Raw | GCS — `gs://<bucket>/raw_data/` | Newline-delimited JSON dumps of the Strava API |
| Bronze | BigQuery `bronze_layer` | External tables over GCS JSON, schemas defined in `Prefect/flows/schemas/` |
| Silver | BigQuery `silver_layer` | dbt staging models: typed, renamed, unit-normalized (km, mins, km/h). Enforced contracts and tests. |
| Gold | BigQuery `gold_layer` | dbt reporting views aggregating silver data to analytics-ready grain |

## Components

- **Prefect** (`Prefect/flows/`) — orchestrates the raw and bronze layers: fetches activities and athlete from the Strava API, uploads to GCS, ensures BigQuery datasets and external tables exist, then hands off to dbt.
- **dbt** (`dbt/`) — owns the silver and gold transformations. Models live under `dbt/models/{staging,reporting}/`; sources, contracts and tests in `schema.yml` per folder.

## Running locally

```bash
# install dependencies
pip install -r requirements.txt

# set required environment variables in .env
# STRAVA_CLIENT_ID, STRAVA_CLIENT_SECRET, STRAVA_REFRESH_TOKEN, PROJECT, BUCKET

# authenticate to GCP for ADC
gcloud auth application-default login

# run the full pipeline (raw → bronze → silver → gold)
python Prefect/flows/main_flow.py
```

## dbt commands

```bash
cd dbt
dbt parse                                 # validate project config
dbt run --select tag:staging              # build silver
dbt run --select tag:reporting            # build gold
dbt test                                  # run data quality tests
```

## Project layout

```
Prefect/flows/
  main_flow.py               # entry point
  tasks/                     # ingest tasks (Strava API, GCS, BigQuery setup)
  schemas/                   # BigQuery schemas for the bronze external tables
dbt/
  dbt_project.yml
  profiles.yml
  models/
    staging/                 # silver: stg_strava__activities, stg_strava__athlete
    reporting/               # gold: rep_strava__activities (fact_activities)
```
