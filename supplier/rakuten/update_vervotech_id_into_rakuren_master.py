import os
import time
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from concurrent.futures import ThreadPoolExecutor, as_completed

# —————————————
# Configuration
# —————————————
load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
engine       = create_engine(DATABASE_URL, pool_recycle=3600)

API_ACCOUNT  = os.getenv('VERVO_API_ACCOUNT', 'gtrs')
API_KEY      = os.getenv('VERVO_API_KEY',     'b0ae90d7-2507-4751-ba4d-d119827c1ed2')
API_URL      = "https://hotelmapping.vervotech.com/api/3.0/content/GetProviderContentByProviderHotelIds"

# —————————————
# DB helpers
# —————————————
def fetch_pending(conn):
    return conn.execute(text("""
        SELECT Id, hotel_code
          FROM rakuten_master
         WHERE vervotech_id IS NULL OR vervotech_id = 0
    """)).fetchall()

def update_one(conn, row_id, vt_id):
    conn.execute(text("""
        UPDATE rakuten_master
           SET vervotech_id = :vid
         WHERE Id = :rid
    """), {"vid": vt_id, "rid": row_id})

# —————————————
# API helper
# —————————————
def call_api(hotel_code):
    payload = {
        "ProviderHotelIdentifiers": [
            {"ProviderHotelId": hotel_code, "ProviderFamily": "Rakuten"}
        ]
    }
    headers = {
        'accountid':   API_ACCOUNT,
        'apikey':       API_KEY,
        'Content-Type': 'application/json'
    }
    resp = requests.post(API_URL, headers=headers, json=payload, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    try:
        return data["Hotels"][0]["ProviderHotels"][0]["VervotechId"]
    except Exception:
        return None

# —————————————
# Per‐hotel worker
# —————————————
def worker(task):
    row_id, hotel_code = task
    try:
        vt = call_api(hotel_code)
    except Exception as e:
        return (row_id, hotel_code, None, f"API error: {e}")

    if not vt:
        return (row_id, hotel_code, None, "no VervotechId")

    # commit immediately
    try:
        with engine.begin() as conn:
            update_one(conn, row_id, vt)
        return (row_id, hotel_code, vt, None)
    except SQLAlchemyError as e:
        return (row_id, hotel_code, None, f"DB error: {e}")

# —————————————
# Main
# —————————————
def main():
    with engine.connect() as conn:
        rows = fetch_pending(conn)
    print(f"→ {len(rows)} hotels pending…")

    # spin up 10 threads
    with ThreadPoolExecutor(max_workers=10) as exe:
        futures = {exe.submit(worker, (r.Id, r.hotel_code)): r for r in rows}

        for i, fut in enumerate(as_completed(futures), 1):
            row = futures[fut]
            row_id, code, vt, err = fut.result()
            if vt:
                print(f"[{i}/{len(rows)}] {code} → updated {vt}")
            else:
                print(f"[{i}/{len(rows)}] {code} → skipped ({err})")
            time.sleep(0.1)   # gentle pacing

if __name__ == "__main__":
    main()
