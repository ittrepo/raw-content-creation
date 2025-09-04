import requests
import time
import logging
import hashlib
from dotenv import load_dotenv
import os


load_dotenv()

# ---------- CONFIG ----------
API_KEY = os.getenv("HOTELBEDS_API_KEY")
API_SECRET = os.getenv("HOTELBEDS_API_SECRET")
BASE_URL = "https://api.hotelbeds.com/hotel-content-api/1.0/hotels"

OUTPUT_FILE = "hotel_id_list_5.txt"
BATCH_SIZE = 100   # API limit per request


timestamp = str(int(time.time()))
signature_data = f"{API_KEY}{API_SECRET}{timestamp}"
signature = hashlib.sha256(signature_data.encode('utf-8')).hexdigest()

headers = {
    "Api-key": API_KEY,
    "X-Signature": signature,
    "Accept-Encoding": "gzip"
}

def fetch_hotel_ids():
    hotel_ids = []
    start = 203397
    
    while True:
        end = start + BATCH_SIZE - 1
        url = f"{BASE_URL}?fields=all&language=ENG&from={start}&to={end}&useSecondaryLanguage=false&lastUpdateTime=2020-09-04"
        
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(f"❌ Error {response.status_code}: {response.text}")
            break

        data = response.json()

        # Extract hotel IDs
        hotels = data.get("hotels", [])
        for hotel in hotels:
            hotel_ids.append(str(hotel["code"]))

        print(f"✅ Fetched hotels {start} - {end} / {data.get('total')}")

        # Stop when we reach the total
        if end >= data.get("total", 0):
            break

        start = end + 1
    
    return hotel_ids


def save_to_file(hotel_ids):
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(hotel_ids))
    print(f"💾 Saved {len(hotel_ids)} hotel IDs to {OUTPUT_FILE}")


if __name__ == "__main__":
    ids = fetch_hotel_ids()
    save_to_file(ids)
