import os
import json
from datetime import datetime
from sqlalchemy import (
    create_engine, MetaData, Table, Column,
    Integer, String, Float, DateTime
)
from sqlalchemy.exc import IntegrityError

# ─── Configuration ────────────────────────────────────────────────────────────────
BASE_PATH = r"D:/content_for_hotel_json/HotelInfo/rakuten"
DATABASE_URL = "mysql+pymysql://root:@localhost/csvdata01_02102024"
FAILED_IDS_FILE = "get_hotel_id_list.txt"

engine = create_engine(DATABASE_URL)
metadata = MetaData()

# ─── Table Definition ─────────────────────────────────────────────────────────────
rnr_table = Table(
    "rakuten_master", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("vervotech_id", String(50), nullable=True),
    Column("hotel_code", String(50), nullable=False, unique=True),
    Column("hotel_name", String(255), nullable=False),
    Column("latitude", Float, nullable=True),
    Column("longitude", Float, nullable=True),
    Column("address", String(500), nullable=True),
    Column("post_code", String(20), nullable=True),
    Column("stars", String(10), nullable=True),
    Column("country_code", String(5), nullable=True),
    Column("primary_photo", String(500), nullable=True),
    Column("property_type", String(30), nullable=True),
    Column("created_at", DateTime, default=datetime.utcnow),
    Column("modified_on", DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
)

# ─── Create Table (if not exists) ────────────────────────────────────────────────
metadata.create_all(engine)
print("Table `rakuten_master` is ready.")

# ─── Insert JSON Data ─────────────────────────────────────────────────────────────
with engine.begin() as conn, open(FAILED_IDS_FILE, "a", encoding="utf-8") as failed_file:
    for fname in os.listdir(BASE_PATH):
        if not fname.endswith(".json"):
            continue

        path = os.path.join(BASE_PATH, fname)
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        hotel_id = data.get("hotel_id")
        row = {
            "hotel_code": hotel_id,
            "hotel_name": data.get("name"),
            "latitude": data.get("address", {}).get("latitude"),
            "longitude": data.get("address", {}).get("longitude"),
            "address": data.get("address", {}).get("address_line_1"),
            "post_code": data.get("address", {}).get("postal_code"),
            "stars": data.get("star_rating"),
            "country_code": data.get("country_code"),
            "property_type": data.get("property_type"),
            "primary_photo": data.get("primary_photo"),
            "created_at": datetime.utcnow(),
            "modified_on": datetime.utcnow()
        }

        try:
            if row["hotel_name"] is None:
                raise ValueError("Missing required field 'hotel_name'")

            conn.execute(rnr_table.insert().values(**row))
            print(f"Inserted hotel {hotel_id}")

        except (IntegrityError, ValueError) as e:
            print(f"Skipping hotel {hotel_id}: {e}")
            failed_file.write(hotel_id + "\n")

print("All JSON files have been processed.")
