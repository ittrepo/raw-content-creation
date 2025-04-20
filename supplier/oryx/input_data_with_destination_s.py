import requests
import json
from sqlalchemy import create_engine, text
import pandas as pd

# Database configuration
DATABASE_URL = "mysql+pymysql://root:@localhost/csvdata01_02102024"
engine = create_engine(DATABASE_URL)

# Corrected SQL statement with proper closing parenthesis
create_table_sql = """
CREATE TABLE IF NOT EXISTS oryx_destination_id (
    id INT AUTO_INCREMENT PRIMARY KEY,
    giDestinationId VARCHAR(255),
    destinationId VARCHAR(255),
    cityName VARCHAR(255),
    countryName VARCHAR(255),
    stateCode VARCHAR(255),
    countryCode VARCHAR(255)
)
"""
print("Executing SQL:", create_table_sql)  

try:
    with engine.begin() as connection:
        connection.execute(text(create_table_sql))
    print("Table created successfully or already exists.")
except Exception as e:
    print(f"Error creating table: {e}")
    print("Please check the SQL syntax and ensure the database is accessible.")
    exit()

# Fetch existing destinationIds to avoid duplicates
existing_ids = set()
try:
    with engine.connect() as connection:
        query = text("SELECT destinationId FROM oryx_destination_id")
        result = connection.execute(query)
        existing_ids = {row[0] for row in result}
    print(f"Found {len(existing_ids)} existing destination IDs in the database")
except Exception as e:
    print(f"Error fetching existing IDs: {e}")

# API configuration
url = "http://uat-apiv2.giinfotech.ae/api/v2/hotel/destination-info"
headers = {
    'Content-Type': 'application/json',
    'ApiKey': 'MtG5lGOBy06CpnY43AoGsA=='
}

# Read city names from the file with proper encoding
try:
    with open('get_all_city_name.txt', 'r', encoding='utf-8', errors='ignore') as f:
        cities = [line.strip() for line in f if line.strip()]
except FileNotFoundError:
    print("Error: The file 'get_all_city_name.txt' was not found.")
    exit()

# Process each city
total_processed = 0
total_skipped = 0

for city in cities:
    payload = json.dumps({"destination": city})
    try:
        response = requests.post(url, headers=headers, data=payload, timeout=10)
        response.raise_for_status()
        response_json = response.json()
        
        if response_json.get('isSuccess', False):
            data = response_json.get('data', [])
            if data:
                # Filter out records that already exist in the database
                new_records = []
                for record in data:
                    if record.get('destinationId') not in existing_ids:
                        new_records.append(record)
                        # Add to our set to avoid duplicates in subsequent API calls
                        existing_ids.add(record.get('destinationId'))
                
                if new_records:
                    df = pd.DataFrame(new_records)
                    df.to_sql('oryx_destination_id', con=engine, if_exists='append', index=False)
                    print(f"Successfully inserted {len(new_records)} new records for city: {city}")
                    total_processed += len(new_records)
                    total_skipped += (len(data) - len(new_records))
                else:
                    print(f"All {len(data)} records for city {city} already exist in database, skipping.")
                    total_skipped += len(data)
            else:
                print(f"No data found for city: {city}")
        else:
            print(f"Skipping city {city}: API response was not successful.")
    except requests.exceptions.RequestException as e:
        print(f"Request failed for city {city}: {e}")
    except json.JSONDecodeError as e:
        print(f"Failed to decode JSON response for city {city}: {e}")
    except Exception as e:
        print(f"An unexpected error occurred for city {city}: {e}")

print(f"Processing complete. Added {total_processed} new records. Skipped {total_skipped} existing records.")