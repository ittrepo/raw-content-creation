import os
import pandas as pd
import ast
from sqlalchemy import create_engine, MetaData, Table, select, update, insert
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

load_dotenv()

# ——— Setup engine & session ———
conn_str = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
engine = create_engine(conn_str, pool_recycle=1800, pool_pre_ping=True, connect_args={"connect_timeout": 30})
Session = sessionmaker(bind=engine)
session = Session()

# ——— Reflect table ———
meta = MetaData()
meta.reflect(bind=engine, only=["global_hotel_mapping"])
ghm = Table("global_hotel_mapping", meta, autoload_with=engine)

# ——— Read & filter Excel ———
df = pd.read_excel("D:/Rokon/ofc_git/row_content_create/xl_check_db/webbeds/dotw_checked_output_with_country_iso.xlsx")
df = df[df["dotw_present_itt"] == "No"]

# ——— Helper function ———
def clean_val(x):
    return None if pd.isna(x) else x

# ——— Track inserted records ———
inserted_records = []

# ——— Process rows ———
for _, row in df.iterrows():
    raw_id = clean_val(row["Vervotech_HotelCode"])
    if raw_id is None:
        print("❌ Skipping: No Vervotech_HotelCode")
        continue

    try:
        vervotech_id = int(raw_id)
    except Exception:
        print(f"❌ Skipping: Invalid VervotechId: {raw_id}")
        continue

    dotw_code = clean_val(row["DOTW_HotelCode"])
    if dotw_code is None:
        print(f"❌ Skipping VervotechId={vervotech_id}: Missing DOTW_HotelCode")
        continue
    dotw_code = str(dotw_code)

    # — Fetch all rows with this VervotechId —
    rows = session.execute(
        select(ghm).where(ghm.c.VervotechId == vervotech_id)
    ).fetchall()

    if rows:
        updated = False
        for row_item in rows:
            dotw_existing = row_item.dotw

            # Parse existing dotw field if it's a string
            try:
                if isinstance(dotw_existing, str):
                    dotw_existing = ast.literal_eval(dotw_existing)
                if not isinstance(dotw_existing, list):
                    dotw_existing = []
            except Exception:
                dotw_existing = []

            if dotw_code in dotw_existing:
                print(f"⏭️ Duplicate DOTW. Skipping VervotechId={vervotech_id}")
                updated = True
                break

        if not updated:
            # ✅ Append to first matching record
            new_dotw_list = dotw_existing + [dotw_code]
            session.execute(
                update(ghm)
                .where(ghm.c.VervotechId == vervotech_id)
                .values(dotw=new_dotw_list)
            )
            session.commit()
            print(f"✅ Updated dotw for VervotechId={vervotech_id}")
        continue

    # — INSERT if not exists —
    record = {
        "VervotechId":   vervotech_id,
        "dotw":          [dotw_code],
        "GiataCode":     clean_val(row["GIATA_HotelCode"]),
        "Name":          clean_val(row["HotelName"]),
        "AddressLine1":  clean_val(row["HotelAddress"]),
        "CountryCode":   clean_val(row["Country_ISO"]),
        "CountryName":   clean_val(row["Country"]),
        "CityName":      clean_val(row["City"]),
        "ChainName":     clean_val(row["ChainName"]),
        "Latitude":      clean_val(row["Latitude"]),
        "Longitude":     clean_val(row["Longitude"]),
        "Phones":        clean_val(row["ReservationTelephone"]),
    }

    session.execute(insert(ghm).values(**record))
    session.commit()
    inserted_records.append(record)
    print(f"✅ Inserted new VervotechId={vervotech_id} with DOTW={dotw_code}")

# ——— Finalize ———
session.commit()
session.close()

# ——— Summary ———
print("\n📋 Inserted Records:")
for rec in inserted_records:
    print(rec)

print(f"\n⏹ All done. Total newly inserted: {len(inserted_records)}")
