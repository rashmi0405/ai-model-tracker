"""
extract_models.py

Saves today's snapshot of every AI model listed on OpenRouter
(name, prices, context size, and more) into Google Cloud Storage.

This is the "E" (extract) step of the pipeline, and Cloud Storage is our
data lake (the bronze, or raw, layer). We save the API response exactly
as it arrives and never edit it, so we can always rebuild later tables.
"""

# ---------- Imports ----------
import json                              # turn Python data into JSON text
import os                                # read settings from environment variables
import time                              # pause between retries
from datetime import datetime, timezone  # get today's date in UTC

import requests                          # make HTTP calls to the API
from dotenv import load_dotenv           # load the settings in our .env file
from google.cloud import storage         # Google's library for Cloud Storage

# Read .env and put OPENROUTER_API_KEY, GCP_PROJECT, and RAW_BUCKET into
# environment variables, so secrets never have to be written in the code.
load_dotenv()

# ---------- Settings ----------
API_URL = "https://openrouter.ai/api/v1/models"  # the endpoint that lists every model
MIN_EXPECTED_MODELS = 50                          # far fewer than this means something is wrong


# ---------- Step 1: get the data ----------
def fetch_models(retries=4):
    """Call the API and return its JSON response.

    Temporary problems (a network blip, a busy server, a rate limit) are
    retried, waiting a little longer each time. This is called
    "exponential backoff": wait 2s, then 4s, then 8s.
    """
    # Send our API key in the request header, the way OpenRouter expects it.
    headers = {"Authorization": f"Bearer {os.environ['OPENROUTER_API_KEY']}"}

    for attempt in range(1, retries + 1):        # try up to 4 times
        try:
            # timeout=30: give up on one try if the server takes over 30 seconds.
            response = requests.get(API_URL, headers=headers, timeout=30)
        except (requests.ConnectionError, requests.Timeout):
            # Couldn't reach the server at all. Mark it and retry below.
            response = None

        # 200 means success: return the data as Python dicts and lists.
        if response is not None and response.status_code == 200:
            return response.json()

        # Errors like 401 (bad key) or 404 (wrong URL) won't fix themselves,
        # so stop right away with the error instead of retrying.
        # 429 (too many requests) and 5xx (server errors) are temporary, so
        # those skip this and get retried.
        if response is not None and response.status_code < 500 and response.status_code != 429:
            response.raise_for_status()

        # That was the last try, so give up with a clear message.
        if attempt == retries:
            raise RuntimeError(f"OpenRouter API still failing after {retries} tries")

        wait = 2 ** attempt                      # 2, 4, 8 seconds
        print(f"Attempt {attempt} failed, retrying in {wait}s")
        time.sleep(wait)


# ---------- Step 2: check the data ----------
def check(payload):
    """Make sure the response looks right before saving it.

    It's better for the script to fail loudly than to quietly save a
    broken snapshot that later shows wrong numbers on the dashboard.
    Returns the number of models if everything looks good.
    """
    # The API puts the list of models under the key "data".
    models = payload.get("data")
    if not isinstance(models, list):
        raise ValueError("Response has no 'data' list")

    # A sudden drop in the number of models usually means a bad response.
    if len(models) < MIN_EXPECTED_MODELS:
        raise ValueError(f"Only {len(models)} models returned; expected at least {MIN_EXPECTED_MODELS}")

    # Every model needs an ID and prices, or later steps can't use it.
    missing = [m for m in models if not m.get("id") or "pricing" not in m]
    if missing:
        raise ValueError(f"{len(missing)} models are missing an id or pricing")

    return len(models)


# ---------- Step 3: save the data ----------
def save_to_gcs(payload, snapshot_date):
    """Write the raw response to Cloud Storage as one file per day.

    The file path includes the date, for example:
    openrouter/models/snapshot_date=2026-10-02/models.json
    Because each day has its own path, running the script twice on the
    same day overwrites that day's file instead of creating a duplicate.
    """
    # Connect to Cloud Storage. This uses the login from
    # `gcloud auth application-default login`, so no key file is needed.
    client = storage.Client(project=os.environ["GCP_PROJECT"])
    bucket = client.bucket(os.environ["RAW_BUCKET"])

    # A "blob" is one file inside the bucket.
    blob = bucket.blob(f"openrouter/models/snapshot_date={snapshot_date}/models.json")

    # Extra labels stored with the file: when we saved it and where it came from.
    blob.metadata = {"ingested_at": datetime.now(timezone.utc).isoformat(), "source": API_URL}

    # Upload the response as JSON text, exactly as the API sent it.
    blob.upload_from_string(json.dumps(payload), content_type="application/json")

    # Return the full file location so we can print it.
    return f"gs://{bucket.name}/{blob.name}"


# ---------- Run the steps in order ----------
# This block runs only when you start the file directly
# (python extract/extract_models.py), not when another file imports it.
# That lets Airflow reuse these functions later.
if __name__ == "__main__":
    # Use the date in UTC, so the date doesn't depend on your time zone.
    snapshot_date = datetime.now(timezone.utc).date().isoformat()   # e.g. "2026-10-02"

    payload = fetch_models()                 # 1. get today's models from the API
    count = check(payload)                   # 2. stop here if the data looks wrong
    uri = save_to_gcs(payload, snapshot_date)  # 3. save the raw file to the data lake
    print(f"Saved {count} models to {uri}")