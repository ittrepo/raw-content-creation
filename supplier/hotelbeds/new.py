import os
import time
import hashlib
import logging
import requests
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# ── CONFIG ────────────────────────────────────────────────────────────────
load_dotenv()

HOTEL_ID_LIST   = "/var/www/ScirptEngine/Python-Application/Raw-Content-Create-Function/hotelbeds/hotelbeds_hotel_id_list.txt"
SUCCESS_FILE    = "/var/www/ScirptEngine/Python-Application/Raw-Content-Create-Function/hotelbeds/hotelbeds_successful_done_hotel_id_list.txt"
NOT_FOUND_FILE  = "/var/www/ScirptEngine/Python-Application/Raw-Content-Create-Function/hotelbeds/hotelbeds_not_found.txt"
BASE_PATH       = "/var/www/Storage-Contents/Hotel-Supplier-Raw-Contents/hotelbeds"

API_KEY         = os.getenv("HOTELBEDS_API_KEY")
API_SECRET      = os.getenv("HOTELBEDS_API_SECRET")

# Tune these for your rate:
CONCURRENCY     = 15      # number of worker threads
MAX_RETRIES     = 5       # retry attempts on 429
INITIAL_BACKOFF = 1.0     # seconds
BACKOFF_FACTOR  = 2.0     # back-off multiplier

# ── LOGGING ───────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

_success_lock  = Lock()
_notfound_lock = Lock()

def fetch_with_retries(hotel_id: str) -> requests.Response:
    """GET hotel-details, retrying on 429 with exponential back-off."""
    timestamp = str(int(time.time()))
    signature = hashlib.sha256(f"{API_KEY}{API_SECRET}{timestamp}".encode()).hexdigest()
    url = (
        f"https://api.hotelbeds.com/hotel-content-api/1.0/"
        f"hotels/{hotel_id}/details?language=ENG&useSecondaryLanguage=False"
    )
    headers = {
        "Api-key":         API_KEY,
        "X-Signature":     signature,
        "Accept-Encoding": "gzip",
        "Content-Type":    "application/json",
    }

    backoff = INITIAL_BACKOFF
    for attempt in range(1, MAX_RETRIES + 1):
        resp = requests.get(url, headers=headers)
        if resp.status_code == 429:
            logging.warning(
                f"{hotel_id}: HTTP 429 rate limit, retry {attempt}/{MAX_RETRIES} in {backoff:.1f}s"
            )
            time.sleep(backoff)
            backoff *= BACKOFF_FACTOR
            continue
        return resp

    logging.error(
        f"{hotel_id}: giving up after {MAX_RETRIES} retries (last status {resp.status_code})"
    )
    return resp


def save_json_with_backoff(raw_path: str, hotel_id: str) -> bool:
    """
    Fetch with retries, write JSON to disk, and log path.
    Returns True if status_code == 200.
    """
    resp = fetch_with_retries(hotel_id)

    if resp.status_code == 200:
        data = resp.text
    else:
        data = "{}"
        logging.warning(f"{hotel_id}: saving empty JSON due to HTTP {resp.status_code}")

    out_file = os.path.join(raw_path, f"{hotel_id}.json")
    try:
        with open(out_file, "w", encoding="utf-8") as f:
            f.write(data)
        logging.info(f"{hotel_id}.json ? {out_file}")
        return resp.status_code == 200
    except Exception as e:
        logging.error(f"{hotel_id}: error writing file: {e}")
        return False


def process_hotel(hotel_id: str) -> tuple[str, bool]:
    """
    Worker function: logs start, does fetch+save, then logs final result.
    """
    logging.info(f"Starting {hotel_id}")
    success = save_json_with_backoff(BASE_PATH, hotel_id)
    status = "SUCCESS" if success else "FAILED"
    logging.info(f"Processed hotel {hotel_id}: {status}")
    return hotel_id, success


def process_hotels(concurrency: int = CONCURRENCY):
    os.makedirs(BASE_PATH, exist_ok=True)

    if not os.path.exists(HOTEL_ID_LIST):
        logging.error(f"Hotel ID list not found: {HOTEL_ID_LIST}")
        return

    with open(HOTEL_ID_LIST, "r", encoding="utf-8") as f:
        hotel_ids = [line.strip() for line in f if line.strip()]

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {executor.submit(process_hotel, hid): hid for hid in hotel_ids}

        for fut in as_completed(futures):
            hid, ok = fut.result()
            if ok:
                with _success_lock:
                    with open(SUCCESS_FILE, "a", encoding="utf-8") as sf:
                        sf.write(hid + "\n")
                    print(f"Processed {hid} successfully.++++++++++++++++++++++++++++++++++++")
            else:
                with _notfound_lock:
                    with open(NOT_FOUND_FILE, "a", encoding="utf-8") as nf:
                        nf.write(hid + "\n")
                    print(f"Failed to process {hid}.>>>>>>>>>>>>>>>>>>>>")

    logging.info("All hotel processing complete.")


if __name__ == "__main__":
    process_hotels()
