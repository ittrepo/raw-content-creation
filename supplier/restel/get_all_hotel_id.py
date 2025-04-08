from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import pandas as pd

DATABASE_URL = "mysql+pymysql://root:@localhost/csvdata01_02102024"
engine = create_engine(DATABASE_URL)

table = "restel_hotel_destination"
# Function to get all distinct city names and save to a text file
def get_all_city():
    query = text(f"SELECT DISTINCT `hotel_code` FROM {table}")
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    
    # Save to text file (one city name per line)
    df.to_csv("get_all_hotel_code_from_restel.txt", index=False, header=False)

# Call the function
get_all_city()
