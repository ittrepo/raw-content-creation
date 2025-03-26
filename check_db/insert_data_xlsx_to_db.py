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

# Read the Excel file that contains only rows where giata_present = 'No'
df = pd.read_excel('Umrahbooking_updated_only_no.xlsx')

# (Optional) Ensure we filter again if needed:
df_filtered = df[df['giata_present'] == 'No']

df_to_insert = df_filtered.rename(columns={
    'Ratehawk Code': 'ratehawkhotel',
    'Hotel name': 'Name',
    'Address': 'AddressLine1',
    'Country Iso': 'CountryCode',
    'Hotel country': 'CountryName',
    'Hotel city': 'CityName',
    'Star rating': 'Rating',
    'latitude': 'Latitude',
    'longitude': 'Longitude'
})

# Convert DataFrame rows to a list of dictionaries for insertion
records = df_to_insert.to_dict(orient='records')

# Insert the data into the global_hotel_mapping table
with engine.begin() as conn:
    conn.execute(global_hotel_mapping.insert(), records)

print("Data inserted successfully.")
