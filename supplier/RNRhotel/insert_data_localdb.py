# import os
# import json
# from datetime import datetime
# from sqlalchemy import (
#     create_engine, MetaData, Table, Column,
#     Integer, String, Float, DateTime
# )

# # ─── Configuration ────────────────────────────────────────────────────────────────
# BASE_PATH = r"D:/content_for_hotel_json/cdn_row_collection/RNRhotel"
# DATABASE_URL = "mysql+pymysql://root:@localhost/csvdata01_02102024"
# engine = create_engine(DATABASE_URL)
# metadata = MetaData()

# # ─── Table Definition ─────────────────────────────────────────────────────────────
# rnr_table = Table(
#     "rnrhotel_master", metadata,
#     Column("id", Integer, primary_key=True, autoincrement=True),
#     Column("hotel_code", String(50), nullable=False, unique=True),
#     Column("hotel_name", String(255), nullable=False),
#     Column("latitude", Float, nullable=True),
#     Column("longitude", Float, nullable=True),
#     Column("address", String(500), nullable=True),
#     Column("post_code", String(20), nullable=True),
#     Column("email", String(100), nullable=True),
#     Column("phone", String(50), nullable=True),
#     Column("website", String(255), nullable=True),
#     Column("stars", Integer, nullable=True),
#     Column("country", String(5), nullable=False, default="BD"),
#     Column("created_at", DateTime, default=datetime.utcnow),
#     Column("modified_on", DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
# )

# # ─── Create Table (if not exists) ────────────────────────────────────────────────
# metadata.create_all(engine)
# print("Table `RNRhotel_master` is ready.")

# # ─── Insert JSON Data ─────────────────────────────────────────────────────────────
# with engine.begin() as conn:
#     for fname in os.listdir(BASE_PATH):
#         if not fname.endswith(".json"):
#             continue

#         path = os.path.join(BASE_PATH, fname)
#         with open(path, "r", encoding="utf-8") as f:
#             data = json.load(f)

#         # build the row dict
#         row = {
#             "hotel_code": data.get("id"),
#             "hotel_name": data.get("name"),
#             "latitude": data.get("geo_coordinates", {}).get("latitude"),
#             "longitude": data.get("geo_coordinates", {}).get("longitude"),
#             "address": data.get("address"),
#             "post_code": data.get("post_code"),
#             "email": data.get("contacts", {}).get("email"),
#             "phone": data.get("contacts", {}).get("phone"),
#             "website": data.get("contacts", {}).get("webpage"),
#             "stars": data.get("stars"),
#             "country": "BD",
#         }

#         # Perform insert; if you want to skip duplicates on hotel_code, you could add logic here
#         conn.execute(rnr_table.insert().values(**row))
#         print(f"Inserted hotel {row['hotel_code']}")

# print("All JSON files have been processed.")









# import os
# import json
# from queue import Queue
# from threading import Event
# from concurrent.futures import ThreadPoolExecutor
# from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, DateTime
# from sqlalchemy.exc import SQLAlchemyError
# from sqlalchemy.orm import sessionmaker
# from datetime import datetime, timezone
# import itertools

# # ───────────────────────────────
# # CONFIGURATION
# # ───────────────────────────────
# BASE_PATH = r"D:/content_for_hotel_json/HotelInfo/rnrhotel"
# DATABASE_URL = "mysql+pymysql://root:@localhost/csvdata01_02102024"
# FAILED_IDS_FILE = "get_hotel_id_list.txt"
# BATCH_SIZE = 500
# READER_THREADS = 8
# WRITER_THREADS = 4
# QUEUE_MAXSIZE = 5000

# batch_counter = itertools.count(1)
# work_q = Queue(maxsize=QUEUE_MAXSIZE)
# STOP_READING = Event()
# failed_ids = []

# # ───────────────────────────────
# # DATABASE SETUP
# # ───────────────────────────────
# engine = create_engine(
#     DATABASE_URL,
#     pool_size=10,
#     max_overflow=20,
#     pool_pre_ping=True,
#     future=True
# )
# metadata = MetaData()

# rnr_table = Table(
#     "rnrhotel_master", metadata,
#     Column("id", Integer, primary_key=True, autoincrement=True),
#     Column("vervotech_id", String(50), nullable=True),
#     Column("giata_code", String(50), nullable=True),
#     Column("hotel_code", String(50), nullable=False, unique=True),
#     Column("hotel_name", String(255), nullable=False),
#     Column("latitude", Float, nullable=True),
#     Column("longitude", Float, nullable=True),
#     Column("address", String(500), nullable=True),
#     Column("post_code", String(20), nullable=True),
#     Column("stars", String(10), nullable=True),
#     Column("country_code", String(5), nullable=True),
#     Column("primary_photo", String(500), nullable=True),
#     Column("property_type", String(30), nullable=True),
#     Column("created_at", DateTime, default=datetime.utcnow),
#     Column("modified_on", DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
# )

# metadata.create_all(engine)
# Session = sessionmaker(bind=engine)

# print("✅ Table `irix_master_final` is ready.")

# # ───────────────────────────────
# # READER FUNCTION
# # ───────────────────────────────
# def reader_worker(fname):
#     path = os.path.join(BASE_PATH, fname)
#     try:
#         with open(path, "r", encoding="utf-8") as f:
#             data = json.load(f)
#     except Exception:
#         failed_ids.append(fname)
#         return

#     hotel_id = data.get("hotel_id")
#     hotel_name = data.get("name")
#     if not hotel_id or not hotel_name:
#         failed_ids.append(hotel_id or fname)
#         return

#     work_q.put({
#         "hotel_code": hotel_id,
#         "hotel_name": hotel_name,
#         "latitude": data.get("address", {}).get("latitude"),
#         "longitude": data.get("address", {}).get("longitude"),
#         "address": data.get("address", {}).get("address_line_1"),
#         "post_code": data.get("address", {}).get("postal_code"),
#         "stars": data.get("star_rating"),
#         "country_code": data.get("address", {}).get("country_code"),
#         "property_type": data.get("property_type"),
#         "primary_photo": data.get("primary_photo"),
#         "created_at": datetime.now(timezone.utc),
#         "modified_on": datetime.now(timezone.utc),
#     })

# # ───────────────────────────────
# # WRITER FUNCTION
# # ───────────────────────────────
# def writer_worker(batch_size=BATCH_SIZE):
#     session = Session()
#     buffer = []

#     while True:
#         try:
#             item = work_q.get(timeout=1)
#         except:
#             if STOP_READING.is_set():
#                 if buffer:
#                     try:
#                         batch_num = next(batch_counter)
#                         session.execute(rnr_table.insert().prefix_with("IGNORE"), buffer)
#                         session.commit()
#                         print(f"✅ Final Batch {batch_num}: Inserted {len(buffer)} hotels")
#                     except SQLAlchemyError as e:
#                         session.rollback()
#                         print(f"❌ Final Batch insert failed: {e}")
#                 session.close()
#                 return
#             continue

#         buffer.append(item)

#         if len(buffer) >= batch_size:
#             try:
#                 batch_num = next(batch_counter)
#                 session.execute(rnr_table.insert().prefix_with("IGNORE"), buffer)
#                 session.commit()
#                 print(f"✅ Batch {batch_num}: Inserted {len(buffer)} hotels")
#             except SQLAlchemyError as e:
#                 session.rollback()
#                 print(f"❌ Batch insert failed: {e}")
#             buffer.clear()
#         work_q.task_done()

# # ───────────────────────────────
# # MAIN EXECUTION
# # ───────────────────────────────
# if __name__ == "__main__":
#     json_files = [f for f in os.listdir(BASE_PATH) if f.endswith(".json")]
#     print(f"🔄 Starting processing of {len(json_files):,} JSON files...")

#     with ThreadPoolExecutor(max_workers=WRITER_THREADS) as writer_pool:
#         for _ in range(WRITER_THREADS):
#             writer_pool.submit(writer_worker)

#         with ThreadPoolExecutor(max_workers=READER_THREADS) as reader_pool:
#             futures = [reader_pool.submit(reader_worker, fname) for fname in json_files]
#             for f in futures:
#                 f.result()

#     STOP_READING.set()
#     work_q.join()

#     if failed_ids:
#         with open(FAILED_IDS_FILE, "a", encoding="utf-8") as f:
#             for fid in failed_ids:
#                 f.write(f"{fid}\n")
#         print(f"⚠️ Logged {len(failed_ids)} failed JSON files to '{FAILED_IDS_FILE}'")
#     else:
#         print("✅ All files processed successfully.")

#     print("🎉 Done. All hotel data inserted.")




















import os
import json
from datetime import datetime
from sqlalchemy import (
    create_engine, MetaData, Table, Column,
    Integer, String, Float, DateTime
)
from sqlalchemy.exc import IntegrityError

# ─── Configuration ────────────────────────────────────────────────────────────────
BASE_PATH = r"D:/content_for_hotel_json/HotelInfo/rnrhotel"
DATABASE_URL = "mysql+pymysql://root:@localhost/csvdata01_02102024"
FAILED_IDS_FILE = "rnr_hotel_id_list.txt"

engine = create_engine(DATABASE_URL)
metadata = MetaData()

# ─── Table Definition ─────────────────────────────────────────────────────────────
rnrhotel_table = Table(
    "rnrhotel_master", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("vervotech_id", String(50), nullable=True),
    Column("giata_code", String(50), nullable=True),
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
print("Table `rnrhotel_master` is ready.")

# ─── Insert JSON Data ────────────────────────────────────────────────────────────
with engine.connect() as conn, open(FAILED_IDS_FILE, "a", encoding="utf-8") as failed_file:
    trans = conn.begin()
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
            "country_code": data.get("address", {}).get("country_code"),
            "property_type": data.get("property_type"),
            "primary_photo": data.get("primary_photo"),
            "created_at": datetime.utcnow(),
            "modified_on": datetime.utcnow()
        }

        try:
            if row["hotel_name"] is None:
                raise ValueError("Missing required field 'hotel_name'")

            conn.execute(rnrhotel_table.insert().values(**row))
            trans.commit()  # Commit after each insert
            trans = conn.begin()  # Start a new transaction
            print(f"Inserted hotel {hotel_id}")

        except (IntegrityError, ValueError) as e:
            print(f"Skipping hotel {hotel_id}: {e}")
            failed_file.write(hotel_id + "\n")
            trans.rollback()
            trans = conn.begin()

print("All JSON files have been processed.")
