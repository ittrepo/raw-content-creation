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

# API configuration
url = "http://uat-apiv2.giinfotech.ae/api/v2/hotel/destination-info"
headers = {
    'Content-Type': 'application/json',
    'ApiKey': 'MtG5lGOBy06CpnY43AoGsA=='
}

# Read city names from the file with proper encoding
try:
    with open('get_all_city_name_t.txt', 'r', encoding='utf-8', errors='ignore') as f:
        cities = [line.strip() for line in f if line.strip()]
except FileNotFoundError:
    print("Error: The file 'get_all_city_name_t.txt' was not found.")
    exit()

# Process each city
for city in cities:
    payload = json.dumps({"destination": city})
    try:
        response = requests.post(url, headers=headers, data=payload, timeout=10)
        response.raise_for_status()
        response_json = response.json()
        
        if response_json.get('isSuccess', False):
            data = response_json.get('data', [])
            if data:
                df = pd.DataFrame(data)
                df.to_sql('oryx_destination_id', con=engine, if_exists='append', index=False)
                print(f"Successfully inserted {len(data)} records for city: {city}")
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

print("Processing complete.")
