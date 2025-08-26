from sqlalchemy import create_engine, text

# Create engine (no password, local db)
engine = create_engine("mysql+pymysql://root:@localhost/csvdata01_02102024")

# Query table
with engine.connect() as conn:
    result = conn.execute(text("SELECT id FROM ratehawk_2"))

    # Write to file
    with open("ratehawk_id_list.txt", "w") as f:
        for row in result:
            f.write(str(row[0]) + "\n")

print("Data saved to ratehawk_id_list.txt")
