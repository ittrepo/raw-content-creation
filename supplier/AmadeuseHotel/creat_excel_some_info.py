import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

# --- DATABASE CONFIGURATION --- #
username = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')   
port = '3306'          
database = os.getenv('DB_NAME')

# --- CREATE CONNECTION --- #
engine = create_engine(f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}")

# --- SQL QUERY FOR UNIQUE CityName --- #
query = """
SELECT 
    MIN(`AddressLine1`) AS AddressLine1,
    `CityName`,
    MIN(`CountryName`) AS CountryName,
    MIN(`Latitude`) AS Latitude,
    MIN(`Longitude`) AS Longitude
FROM global_hotel_mapping
WHERE CountryCode = 'AE'
GROUP BY `CityName`;
"""

# --- READ DATA TO DATAFRAME --- #
df = pd.read_sql(query, engine)

# --- SAVE TO EXCEL --- #
df.to_excel("hotels_cities.xlsx", index=False)

print("Unique city data saved to hotels_cities.xlsx")
