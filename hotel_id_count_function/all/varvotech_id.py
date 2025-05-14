from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import pandas as pd

# Load environment variables
load_dotenv()

# Get DB credentials from environment variables
db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')

# Database table name
table = "global_hotel_mapping"

# Create SQLAlchemy engine
connection_url = f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}"
engine = create_engine(connection_url)

# Query to fetch distinct VervotechId
query = f"SELECT DISTINCT VervotechId FROM {table} WHERE VervotechId IS NOT NULL"

# Read data into DataFrame
df = pd.read_sql(query, engine)

# Save to text file (one ID per line)
output_path = "varvotech_id_list.txt"
df["VervotechId"].dropna().astype(str).to_csv(output_path, index=False, header=False)

print(f"✅ Done. VervotechId list saved to: {output_path}")
