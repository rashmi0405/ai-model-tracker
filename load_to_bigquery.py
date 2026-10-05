"""
load_to_bigquery.py

Loads one day's raw snapshot from Cloud Storage into BigQuery:
one row per model per day, in the table ai_models_raw.model_snapshots.

Run it for today:          python load/load_to_bigquery.py
Run it for another day:    python load/load_to_bigquery.py --date 2026-10-02
"""

# ---------- Imports ----------
import argparse                          # read --date from the command line
import json                              # read and write JSON text
import os                                # read settings from environment variables
from datetime import datetime, timezone  # today's date and the load time, in UTC

from dotenv import load_dotenv           # load the settings in our .env file
from google.cloud import bigquery, storage

load_dotenv()

# ---------- Settings ----------
PROJECT = os.environ["GCP_PROJECT"]
BUCKET = os.environ["RAW_BUCKET"]
TABLE_ID = f"{PROJECT}.ai_models_raw.model_snapshots"   # project.dataset.table

# The table's columns. Each model's full record is kept as raw JSON text, so a
# new field from the API never breaks this load (protection against schema drift).
SCHEMA = [
    bigquery.SchemaField("snapshot_date", "DATE", mode="REQUIRED"),   # which day's snapshot
    bigquery.SchemaField("model_id", "STRING", mode="REQUIRED"),      # e.g. "openai/gpt-4o"
    bigquery.SchemaField("raw_json", "STRING", mode="REQUIRED"),      # the model's full record
    bigquery.SchemaField("source_file", "STRING"),                    # where the row came from
    bigquery.SchemaField("loaded_at", "TIMESTAMP"),                   # when it was loaded
]


# ---------- Step 1: make sure the table exists ----------
def ensure_table(bq):
    """Create the table the first time; do nothing if it already exists.

    Partitioned by snapshot_date: each day is stored separately, so a query
    for one day reads only that day, and we can replace one day at a time.
    Clustered by model_id: rows for the same model are stored together,
    so filtering on a model reads less data.
    """
    table = bigquery.Table(TABLE_ID, schema=SCHEMA)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY, field="snapshot_date"
    )
    table.clustering_fields = ["model_id"]
    bq.create_table(table, exists_ok=True)


# ---------- Step 2: read the raw file from the data lake ----------
def read_snapshot(snapshot_date):
    """Download that day's raw JSON file from Cloud Storage."""
    path = f"openrouter/models/snapshot_date={snapshot_date}/models.json"
    blob = storage.Client(project=PROJECT).bucket(BUCKET).blob(path)
    if not blob.exists():
        raise FileNotFoundError(f"No snapshot for {snapshot_date}. Run the extract script first.")
    return json.loads(blob.download_as_text()), f"gs://{BUCKET}/{path}"


# ---------- Step 3: turn the file into table rows ----------
def to_rows(payload, snapshot_date, source_file):
    """One row per model. The model's record is stored as JSON text."""
    loaded_at = datetime.now(timezone.utc).isoformat()
    rows = [
        {
            "snapshot_date": snapshot_date,
            "model_id": model["id"],
            "raw_json": json.dumps(model),
            "source_file": source_file,
            "loaded_at": loaded_at,
        }
        for model in payload["data"]
    ]

    # Quality check: each model should appear only once per day.
    ids = [r["model_id"] for r in rows]
    if len(ids) != len(set(ids)):
        raise ValueError(f"{len(ids) - len(set(ids))} duplicate model_ids in the {snapshot_date} snapshot")
    return rows


# ---------- Step 4: load the rows, replacing only that day ----------
def load_partition(bq, rows, snapshot_date):
    """Load into that day's partition only, replacing anything already there.

    "model_snapshots$20261002" means "just the 2026-10-02 partition", and
    WRITE_TRUNCATE means "replace it". So running this twice for the same
    day gives the same result instead of duplicate rows (idempotent).
    """
    partition = f"{TABLE_ID}${snapshot_date.replace('-', '')}"
    job_config = bigquery.LoadJobConfig(
        schema=SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    job = bq.load_table_from_json(rows, partition, job_config=job_config)
    job.result()                         # wait for the load job to finish
    return job.output_rows


# ---------- Run the steps in order ----------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="Snapshot date, YYYY-MM-DD (default: today in UTC)")
    args = parser.parse_args()
    snapshot_date = args.date or datetime.now(timezone.utc).date().isoformat()

    bq = bigquery.Client(project=PROJECT)
    ensure_table(bq)                                        # 1. table exists
    payload, source_file = read_snapshot(snapshot_date)     # 2. read raw file
    rows = to_rows(payload, snapshot_date, source_file)     # 3. build rows + check
    loaded = load_partition(bq, rows, snapshot_date)        # 4. replace that day
    print(f"Loaded {loaded} rows into {TABLE_ID} for {snapshot_date}")
