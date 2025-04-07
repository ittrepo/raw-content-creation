from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import pandas as pd


# Table name
table = "oryx_destination_id"

DATABASE_URL = "mysql+pymysql://root:@localhost/csvdata01_02102024"
engine = create_engine(DATABASE_URL)


# Function to get all distinct city names and save to a text file
def get_all_destinationId():
    query = text(f"SELECT DISTINCT `destinationId` FROM {table}")
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    
    # Save to text file 
    df.to_csv("get_all_destination_id.txt", index=False, header=False)

# Call the function
get_all_destinationId()
