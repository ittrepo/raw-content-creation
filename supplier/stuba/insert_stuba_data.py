import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table, select, update
from sqlalchemy.orm import sessionmaker

# Load environment variables
load_dotenv()

# Database credentials from environment
DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')

# Connection URI and engine
DATABASE_URI = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
engine = create_engine(DATABASE_URI)
Session = sessionmaker(bind=engine)
session = Session()

# Table metadata
metadata = MetaData()
table_name = 'global_hotel_mapping_copy'
table = Table(table_name, metadata, autoload_with=engine)

# Columns to update
columns_to_process = ['stuba', 'stuba_a', 'stuba_b', 'stuba_c', 'stuba_d', 'stuba_e']

# Directory containing JSON files
base_dir = r"D:\content_for_hotel_json\HotelInfo\stuba_travelGX"
all_files = os.listdir(base_dir)

# Iterate through each row in the table
rows = session.execute(select(table.c.Id, *[table.c[col] for col in columns_to_process])).fetchall()
for row in rows:
    record_id = row.Id
    updates = {}

    for col in columns_to_process:
        lookup_value = getattr(row, col)
        if not lookup_value:
            continue

        # Find matching JSON file ending with '#{lookup_value}.json'
        suffix = f"#{lookup_value}.json"
        matches = [fname for fname in all_files if fname.endswith(suffix)]
        if matches:
            # Extract filename without extension
            filename = matches[0]
            key = os.path.splitext(filename)[0]
            updates[col] = key

    # Apply updates if any
    if updates:
        stmt = (
            update(table)
            .where(table.c.Id == record_id)
            .values(**updates)
        )
        session.execute(stmt)
        session.commit()  # commit immediately after update
        # Show update messages for each column
        for col, new_val in updates.items():
            print(f"Record {record_id}: column '{col}' updated to '{new_val}'")

print("Global hotel mapping table has been fully processed.")
