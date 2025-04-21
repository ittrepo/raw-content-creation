from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, Table, MetaData, update, select
from sqlalchemy.orm import sessionmaker

# Load environment variables from .env file
load_dotenv()

# Get database credentials from environment variables
db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')

# Database connection string
DATABASE_URI = f'mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}'

# Create an engine
engine = create_engine(DATABASE_URI)

# Create a configured "Session" class
Session = sessionmaker(bind=engine)

# Create a Session
session = Session()

# Reflect the existing database into a new model
metadata = MetaData()
metadata.bind = engine

# Reflect the tables
table_2 = Table('oryx_destination_id', metadata, autoload_with=engine)
table_1 = Table('oryx_systemid_master', metadata, autoload_with=engine)

try:
    # Select all rows from table_1
    rows_to_update = session.query(table_1).all()

    for row in rows_to_update:
        # Select the corresponding row from table_2
        corresponding_row = session.query(table_2).filter(table_2.c.destinationId == row.destinationId).first()

        if corresponding_row:
            # Update the row in table_1
            update_stmt = update(table_1).where(table_1.c.destinationId == row.destinationId).values(
                countryName=corresponding_row.countryName,
                countryCode=corresponding_row.countryCode
            )
            session.execute(update_stmt)
            session.commit()
            print(f"Update successful for destinationId: {row.destinationId}")
        else:
            print(f"No matching row found in table_2 for destinationId: {row.destinationId}")

except Exception as e:
    print(f"Error: {e}")
    session.rollback()
finally:
    # Close the session
    session.close()
