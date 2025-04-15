import os
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection details
db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')

# Create SQLAlchemy engine
connection_string = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}"
engine = create_engine(
    connection_string,
    pool_recycle=1800,
    pool_pre_ping=True,
    connect_args={"connect_timeout": 30}
)

# Reflect the database metadata and get the table
metadata = MetaData()
metadata.reflect(bind=engine)
global_hotel_mapping = Table("global_hotel_mapping", metadata, autoload_with=engine)

# Create a session
Session = sessionmaker(bind=engine)
session = Session()

# Read the Excel file
df = pd.read_excel('get_new_file.xlsx')

# Filter for rows where 'find' == 'No'
df_filtered = df[df['find'] == 'No']

# Rename columns to match your DB schema
df_to_insert = df_filtered.rename(columns={
    'HotelCode': 'juniperhotel',
    'HotelName': 'Name',
    'Address': 'AddressLine1',
    'PostalCode': 'PostalCode',
    'Category': 'Rating',
    'Chain': 'ChainName',
    'HotelPhone': 'Phones',
    'HotelEmail': 'Emails',
    'CountryISO': 'CountryCode',
    'Country': 'CountryName',
    'Zone': 'CityName',
    'latitude': 'Latitude',
    'longitude': 'Longitude'
})

# Add extra columns with fixed values
df_to_insert['mapStatus'] = 'Pending'
df_to_insert['contentUpdateStatus'] = 'Done'

# ✅ Replace NaN with None to avoid SQL errors
df_to_insert = df_to_insert.where(pd.notnull(df_to_insert), None)

# Convert DataFrame to list of dicts for bulk insertion
records = df_to_insert.to_dict(orient='records')

# Insert into the database
with engine.begin() as conn:
    conn.execute(global_hotel_mapping.insert(), records)

print("Data inserted successfully.")










