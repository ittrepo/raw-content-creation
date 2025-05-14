import os
import json
import time
import requests
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table, select, update
from sqlalchemy.orm import sessionmaker

# ─── CONFIG & ENV LOADING ──────────────────────────────────────────────────────
load_dotenv()  # expects DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, LOGIN_URL

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
LOGIN_URL = os.getenv("LOGIN_URL")  # e.g. https://b2bapiutils.benzyinfotech.com/Utils/Signature

OUTPUT_DIR = Path(os.getenv("OUTPUT_PATH", r"D:\content_for_hotel_json\cdn_row_collection\akbar"))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

API_BASE = "https://travelportalapi.benzyinfotech.com/api/hotels"

# ─── AUTH HELPER ────────────────────────────────────────────────────────────────
def get_auth_headers():
    """Get authentication headers by logging in once"""
    login_payload = {
        "MerchantID": os.getenv("MERCHANT_ID", "300"),
        "ApiKey": os.getenv("API_KEY", "kXAY9yHARK"),
        "ClientID": os.getenv("CLIENT_ID", "bitest"),
        "Password": os.getenv("PASSWORD", "staging@1"),
        "AgentCode": "",
        "BrowserKey": os.getenv("BROWSER_KEY", "caecd3cd30225512c1811070dce615c1")
    }
    response = requests.post(LOGIN_URL, json=login_payload)
    response.raise_for_status()
    auth_data = response.json()
    return {
        "Authorization": f"Bearer {auth_data['Token']}",
        "user-session-key": auth_data["TUI"]
    }

# ─── DATABASE SETUP ─────────────────────────────────────────────────────────────
engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}")
metadata = MetaData()  # no bind parameter
location_table = Table(
    "akbar_location_master",
    metadata,
    autoload_with=engine
)
Session = sessionmaker(bind=engine)
session = Session()

# ─── SEARCH HELPERS ────────────────────────────────────────────────────────────
def search_init(lat, long, country, auth_headers):
    payload = {
      "geoCode": {"lat": lat, "long": long},
      "destinationCountryCode": country,
      "currency": "INR",
      "culture": "en-US",
      "checkIn": "05/20/2025",
      "checkOut": "05/22/2025",
      "rooms": [{"adults": "2", "children": "0", "childAges": []}],
      "agentCode": "",
      "nationality": "IN",
      "countryOfResidence": "IN",
      "channelId": "b2bsaudideals",
      "affiliateRegion": "B2B_India",
      "segmentId": "",
      "companyId": "1",
      "gstPercentage": 0,
      "tdsPercentage": 0
    }
    headers = {"Content-Type": "application/json", **auth_headers}
    resp = requests.post(f"{API_BASE}/search/init", headers=headers, json=payload)
    resp.raise_for_status()
    return resp.json()["searchId"]


def fetch_results(search_id, auth_headers, trace_key=None):
    url = f"{API_BASE}/search/result/{search_id}/content?limit=200&offset=0"
    headers = {"Accept-Encoding": "gzip, deflate", **auth_headers}
    if trace_key:
        headers["search-tracing-key"] = trace_key
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()

# ─── MAIN LOOP ─────────────────────────────────────────────────────────────────

# Get auth headers once
auth_headers = get_auth_headers()

# Only select rows where status is not 'OK'
stmt = select(
    location_table.c.lat,
    location_table.c.long,
    location_table.c.country,
    location_table.c.status
).where(location_table.c.status != 'OK')

for lat, long_, country, status in session.execute(stmt):
    country = country.strip().upper()
    try:
        search_id = search_init(lat, long_, country, auth_headers)
        print(f"[INIT] {lat},{long_} → searchId={search_id}")
        time.sleep(1)
        result = fetch_results(search_id, auth_headers)
        hotels = result.get("hotels", [])
        for hotel in hotels:
            hid = hotel.get("id")
            if not hid:
                continue
            out_file = OUTPUT_DIR / f"{hid}.json"
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(hotel, f, ensure_ascii=False, indent=2)
            print(f"  → saved hotel {hid} to {out_file}")

        # After processing, update status to 'OK'
        upd = (
            update(location_table)
            .where(
                location_table.c.lat == lat,
                location_table.c.long == long_,
                location_table.c.country == country
            )
            .values(status='OK')
        )
        session.execute(upd)
        session.commit()
        print(f"[UPDATE] status set to OK for {lat},{long_},{country}")

    except Exception as e:
        print(f"[ERROR] at {lat},{long_},{country}: {e}")
session.close()
