import os
import json
from datetime import datetime
from sqlalchemy import (
    create_engine, MetaData, Table, Column,
    Integer, String, Float, DateTime
)

# ─── Configuration ────────────────────────────────────────────────────────────────
BASE_PATH = r"D:/content_for_hotel_json/cdn_row_collection/RNRhotel"
DATABASE_URL = "mysql+pymysql://root:@localhost/csvdata01_02102024"
engine = create_engine(DATABASE_URL)
metadata = MetaData()

# ─── Table Definition ─────────────────────────────────────────────────────────────
rnr_table = Table(
    "rnrhotel_master", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("hotel_code", String(50), nullable=False, unique=True),
    Column("hotel_name", String(255), nullable=False),
    Column("latitude", Float, nullable=True),
    Column("longitude", Float, nullable=True),
    Column("address", String(500), nullable=True),
    Column("post_code", String(20), nullable=True),
    Column("email", String(100), nullable=True),
    Column("phone", String(50), nullable=True),
    Column("website", String(255), nullable=True),
    Column("stars", Integer, nullable=True),
    Column("country", String(5), nullable=False, default="BD"),
    Column("created_at", DateTime, default=datetime.utcnow),
    Column("modified_on", DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
)

# ─── Create Table (if not exists) ────────────────────────────────────────────────
metadata.create_all(engine)
print("Table `RNRhotel_master` is ready.")

# ─── Insert JSON Data ─────────────────────────────────────────────────────────────
with engine.begin() as conn:
    for fname in os.listdir(BASE_PATH):
        if not fname.endswith(".json"):
            continue

        path = os.path.join(BASE_PATH, fname)
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # build the row dict
        row = {
            "hotel_code": data.get("id"),
            "hotel_name": data.get("name"),
            "latitude": data.get("geo_coordinates", {}).get("latitude"),
            "longitude": data.get("geo_coordinates", {}).get("longitude"),
            "address": data.get("address"),
            "post_code": data.get("post_code"),
            "email": data.get("contacts", {}).get("email"),
            "phone": data.get("contacts", {}).get("phone"),
            "website": data.get("contacts", {}).get("webpage"),
            "stars": data.get("stars"),
            "country": "BD",
        }

        # Perform insert; if you want to skip duplicates on hotel_code, you could add logic here
        conn.execute(rnr_table.insert().values(**row))
        print(f"Inserted hotel {row['hotel_code']}")

print("All JSON files have been processed.")
