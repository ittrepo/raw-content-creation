from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import pandas as pd

# Load environment variables from .env file
load_dotenv()

# Get database credentials from environment variables
db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')

# Table name
table = "global_hotel_mapping"

# Create the connection URL and engine
connection_url = f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}"
engine = create_engine(connection_url)

# Function to get all distinct city names and save to a text file
def get_all_city():
    query = text(f"SELECT DISTINCT `CityName` FROM {table} WHERE `CountryCode` = 'SA'")
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    
    # Save to text file (one city name per line)
    df.to_csv("get_all_city_name_SA.txt", index=False, header=False)

# Call the function
get_all_city()
