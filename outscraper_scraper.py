# -*- coding: utf-8 -*-
"""
Outscraper — Manufacturing subtypes, ZIP‑level queries, max speed.
- County-level queries from uscounties.csv
- Multiple keywords (20+), max results per query (500), batches of 25
- Async submit → parallel polling → incremental CSV + checkpoint
- Final output: Company Name, Website — deduped by website, drops empty websites
"""

import os
import json
import time
import math
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from outscraper import ApiClient
import pandas as pd
from tqdm import tqdm

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

OUTPUT_DIR = "output"
COUNTIES_PATH = "uscounties.csv"  # county_full, state_name
BASE_FILENAME = "manufacturing_outscraper"

# 10 manufacturing subtypes provided by Outscraper
# adjust this list as needed, must reflect real place categories
# ─────────────────────────────────────────────
# LOAD KEYWORDS FROM CSV
# ─────────────────────────────────────────────

MANUFACTURERS_PATH = "Manufacturers.csv"

manufacturers_df = pd.read_csv(MANUFACTURERS_PATH)

# Normalize columns
manufacturers_df.columns = manufacturers_df.columns.str.strip()

# Take only rows that are NOT done
manufacturers_df = manufacturers_df[
    manufacturers_df["Status"].str.lower() != "done"
]

# Extract keywords
KEYWORDS = (
    manufacturers_df["Type"]
    .dropna()
    .astype(str)
    .str.strip()
    .tolist()
)

print(f"Loaded {len(KEYWORDS)} keywords from Manufacturers.csv")

OUTSCRAPER_API_KEY = os.environ.get(
    "OUTSCRAPER_API_KEY",
    "MWJjYjRmMGM2MThiNDg5MWI5ZjZlZGIyYTQyOWNkNGF8ZWQzZDhhOTdjZA",
)
client = ApiClient(api_key=OUTSCRAPER_API_KEY)

# Max results per query (Google Maps ~400 cap per search)
LIMIT_PER_QUERY = 500  # Increased from 500 to max allowed
BATCH_SIZE = 25  # Outscraper max per request

# Submit: minimal delay to avoid rate limits
SLEEP_BETWEEN_SUBMIT = 0.3

# Polling: parallel workers (each uses own client to avoid lock)
POLL_WORKERS = 20
POLL_INTERVAL = 15
POLL_MAX_ATTEMPTS = 60  # ~15 min max wait per round

CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, f"{BASE_FILENAME}_checkpoint.json")
RESET_CHECKPOINT = True

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ─────────────────────────────────────────────
# LOAD ZIP CODES (more granular than counties)
# ─────────────────────────────────────────────

zip_path = "zip_code_database.csv"
zip_df = pd.read_csv(zip_path, dtype=str)
# keep only valid zip/state pairs
zip_df = zip_df[zip_df["zip"].notna() & zip_df["state"].notna()]
zip_df = zip_df.drop_duplicates(subset=["zip", "state"])

# Generate queries for each keyword × ZIP combination
all_queries = []
for keyword in KEYWORDS:
    for _, row in zip_df.iterrows():
        all_queries.append({
            "query": f"{keyword} in {row['zip']}, {row['state']}, USA",
            "keyword": keyword,
            "zip": row['zip'],
            "state": row['state'],
        })

print(f"Keywords: {len(KEYWORDS)} ({', '.join(KEYWORDS[:3])}...)")
print(f"ZIP codes: {len(zip_df)} → Total queries: {len(all_queries)}")
print(f"Batches: {math.ceil(len(all_queries) / BATCH_SIZE)}")

# ─────────────────────────────────────────────
# CHECKPOINT
# ─────────────────────────────────────────────

completed_queries = set()
if not RESET_CHECKPOINT and os.path.exists(CHECKPOINT_FILE):
    with open(CHECKPOINT_FILE, "r") as f:
        completed_queries = set(json.load(f).get("completed_queries", []))
    print(f"Resumed: {len(completed_queries)} done → ", end="")

pending_queries = [q for q in all_queries if q["query"] not in completed_queries]
print(f"{len(pending_queries)} pending")

if not pending_queries:
    print("No pending queries. Set RESET_CHECKPOINT=True or delete checkpoint to re-run.")
    exit(0)

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

_csv_lock = threading.Lock()
_checkpoint_lock = threading.Lock()


def save_checkpoint():
    with _checkpoint_lock:
        tmp = CHECKPOINT_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"completed_queries": list(completed_queries), "updated_at": datetime.now().isoformat()}, f)
        os.replace(tmp, CHECKPOINT_FILE)


def append_to_csv(rows, filepath):
    if not rows:
        return
    with _csv_lock:
        df = pd.DataFrame(rows)
        write_header = not os.path.exists(filepath)
        df.to_csv(filepath, mode="a", header=write_header, index=False)


def _parse_county_state(query_str: str) -> str:
    """Parse 'X in County, State, USA' → 'County, State'."""
    if " in " not in query_str:
        return ""
    part = query_str.split(" in ", 1)[-1]
    if part.lower().endswith(", usa"):
        part = part[:-5]
    return part.strip()


def extract_places(result, query_info_list: list):
    """Extract rows with Company Name and Website only."""
    rows = []
    if not result:
        return rows
    groups = result if isinstance(result, list) else [result]
    for i, group in enumerate(groups):
        if not isinstance(group, list):
            group = [group]
        for place in group:
            if not isinstance(place, dict):
                continue
            website = (place.get("site") or place.get("website", "") or "").strip()
            name = (place.get("name", "") or "").strip()
            
            rows.append({
                "Company Name": name,
                "Website": website,
            })
    return rows


def poll_one(job, api_key):
    """Poll a single job (own client for thread safety)."""
    try:
        c = ApiClient(api_key=api_key)
        return job, c.get_request_archive(job["request_id"])
    except Exception as e:
        return job, {"_error": str(e)}


# ─────────────────────────────────────────────
# SUBMIT ALL BATCHES (async)
# ─────────────────────────────────────────────

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_csv = os.path.join(OUTPUT_DIR, f"{BASE_FILENAME}_{timestamp}.csv")

batches = [pending_queries[i : i + BATCH_SIZE] for i in range(0, len(pending_queries), BATCH_SIZE)]
in_flight = []

print("\nSubmitting async batches...")
for batch in tqdm(batches, desc="Submit"):
    try:
        # Extract query strings for API call
        batch_queries = [q["query"] for q in batch]
        r = client.google_maps_search(
            batch_queries,
            limit=LIMIT_PER_QUERY,
            language="en",
            region="us",
            async_request=True,
        )
        rid = r.get("id")
        if rid:
            in_flight.append({"request_id": rid, "queries": batch})
    except Exception as e:
        print(f"Submit error: {e}")
    time.sleep(SLEEP_BETWEEN_SUBMIT)

print(f"Submitted {len(in_flight)} batches\n")

# ─────────────────────────────────────────────
# POLL IN PARALLEL (multiple clients = no lock)
# ─────────────────────────────────────────────

total_places = 0
attempt = 0
initial_wait = min(45, 2 * len(in_flight))
print(f"Waiting {initial_wait}s for processing to start...")
time.sleep(initial_wait)

while in_flight and attempt < POLL_MAX_ATTEMPTS:
    attempt += 1
    print(f"Poll round {attempt} — {len(in_flight)} batches in flight")

    next_round = []
    with ThreadPoolExecutor(max_workers=min(POLL_WORKERS, len(in_flight))) as ex:
        futures = {ex.submit(poll_one, job, OUTSCRAPER_API_KEY): job for job in in_flight}
        for fut in tqdm(as_completed(futures), total=len(futures), desc="Poll", leave=False):
            job, result = fut.result()
            if result.get("_error"):
                next_round.append(job)
                continue
            status = str(result.get("status", "")).lower()
            if status == "success":
                data = result.get("data", result.get("results", []))
                places = extract_places(data, job["queries"])
                if places:
                    append_to_csv(places, output_csv)
                    total_places += len(places)
                for q in job["queries"]:
                    completed_queries.add(q["query"])
                save_checkpoint()
            elif status in ("pending", "running", "in progress"):
                next_round.append(job)
            else:
                print(f"  Batch status: {status}")

    in_flight = next_round
    if in_flight:
        time.sleep(POLL_INTERVAL)

if in_flight:
    print(f"Timed out: {len(in_flight)} batches still pending")

# ─────────────────────────────────────────────
# DEDUPE BY WEBSITE URL (normalized), DROP EMPTY WEBSITES
# ─────────────────────────────────────────────

print("\nDeduplicating by website (dropping empty websites)...")
if os.path.exists(output_csv):
    df_out = pd.read_csv(output_csv, dtype=str).fillna("")
    raw_count = len(df_out)
    
    # Drop rows with empty website
    df_out = df_out[df_out["Website"].str.strip() != ""]
    with_website_count = len(df_out)
    
    # Normalize website for deduping (lowercase, strip protocol/www/trailing slash)
    df_out["_website_norm"] = (
        df_out["Website"]
        .str.lower()
        .str.replace(r"^https?://", "", regex=True)
        .str.replace(r"^www\.", "", regex=True)
        .str.replace(r"/+$", "", regex=True)
        .str.strip()
    )
    
    # Dedupe by website
    df_out = df_out.drop_duplicates(subset=["_website_norm"], keep="first")
    df_clean = df_out.drop(columns=["_website_norm"])
    df_clean = df_clean.sort_values("Company Name").reset_index(drop=True)
    
    deduped_path = output_csv.replace(".csv", "_deduped.csv")
    df_clean.to_csv(deduped_path, index=False)
    print(f"  Raw: {raw_count} → With website: {with_website_count} → Unique: {len(df_clean)} → {deduped_path}")
else:
    deduped_path = output_csv
    df_out = pd.DataFrame()

# ─────────────────────────────────────────────
# SUMMARY
# ─────────────────────────────────────────────

print("\n" + "─" * 50)
print("Done.")
print(f"  Queries completed: {len(completed_queries)} / {len(all_queries)}")
print(f"  Total rows written: {total_places}")
if df_out is not None and len(df_out) > 0:
    print(f"  Unique places (deduped): {len(df_out)}")
print(f"  Keywords used: {len(KEYWORDS)}")
print(f"  Estimated potential: {len(all_queries)} queries × 500 results = {len(all_queries) * 500:,}")
print(f"  Output: {output_csv}")
print(f"  Deduped: {deduped_path}")
print(f"  Checkpoint: {CHECKPOINT_FILE}")
