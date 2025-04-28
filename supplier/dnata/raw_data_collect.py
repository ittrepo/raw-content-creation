import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import create_engine, MetaData, Table, select, update, or_
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.exc import SQLAlchemyError
import requests
import json
import uuid
import os
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

# Print available tables
# print("Available tables:", metadata.tables.keys())

# Check if the table exists
if 'country_info' in metadata.tables:
    global_hotel_mapping = Table("country_info", metadata, autoload_with=engine)
    # Print column names
    # print("Columns in country_info table:", global_hotel_mapping.columns.keys())
else:
    # print("Table 'country_info' does not exist.")
    exit()

# Create a scoped session for thread-safe operations
session_factory = sessionmaker(bind=engine)
Session = scoped_session(session_factory)

def get_user_auth():
    # Authenticate the user and get the session ID
    user_name = os.getenv('DNATA_USERNAME')
    password = os.getenv('DNATA_PASSWORD')
    company_code = os.getenv('DNATA_COMPANYCODE')
    url = "https://gmtmsuat.provesio.com//api/v1/auth/login"
    payload = json.dumps({
        "userName": user_name,
        "password": password,
        "companyCode": company_code
    })
    headers = {
        'Content-Type': 'application/json',
        'X-API-KEY': '7453cc1d-af64-4259-bfca-cf36e167e134'
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    if response.status_code == 200:
        return response.json()
    else:
        # print(f"Error: Unable to authenticate. Status code: {response.status_code}, Response: {response.text}")
        return None

def get_hotel_id(session_id, conversation_id, country, city):
    url = "https://gmtmsuat.provesio.com//api/v1/hotel/search"
    payload = json.dumps({
        "country": country,
        "city": city,
        "checkIn": "2025-06-15",
        "checkOut": "2025-06-30",
        "rooms": [
            {
                "adult": 2,
                "child": 1,
                "roomIndex": 1,
                "childAge": [
                    6
                ]
            }
        ],
        "travelerCountryOfResidence": "UNITED ARAB EMIRATES,AE",
        "travelerNationality": "UNITED ARAB EMIRATES,AE",
        "culture": "en",
        "additionalProperties": [],
        "filters": {
            "currency": "AED",
            "minStarRating": 0,
            "availableHotelsOnly": True,
            "payAtHotelRates": False
        }
    })
    headers = {
        'Content-Type': 'application/json',
        'sessionId': session_id,
        'conversationId': conversation_id
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: Unable to get hotel IDs for {city}, {country}. Status code: {response.status_code}, Response: {response.text}")
        return None

def get_hotel_details(session_id, conversation_id, hotel_key, search_key):
    url = "https://gmtmsuat.provesio.com//api/v1/hotel/details"
    payload = json.dumps({
        "hotelKey": hotel_key,
        "searchKey": search_key,
        "culture": "en"
    })
    headers = {
        'Content-Type': 'application/json',
        'sessionId': session_id,
        'conversationId': conversation_id,
        'X-API-KEY': '7453cc1d-af64-4259-bfca-cf36e167e134'
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: Unable to get hotel details. Status code: {response.status_code}, Response: {response.text}")
        return None

def save_property_info_to_json(property_info, provider_hotel_id, directory):
    # Ensure the directory exists
    os.makedirs(directory, exist_ok=True)
    # Create the file path
    file_path = os.path.join(directory, f"{provider_hotel_id}.json")
    # Write the property_info to the file
    with open(file_path, 'w') as file:
        json.dump(property_info, file, indent=4)
    print(f"✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅ Saved {file_path}")

def process_city(city_name, country_name, session_id, conversation_id, directory):
    session = Session()
    try:
        hotel_response = get_hotel_id(session_id, conversation_id, country_name, city_name)
        if hotel_response and 'data' in hotel_response:
            search_key = hotel_response['commonData'].get('searchKey')
            if not search_key:
                print(f"Error: 'searchKey' not found for {city_name}, {country_name}")
                return

            for hotel in hotel_response['data']:
                provider_hotel_id = hotel['propertyInfo']['providerHotelId']
                hotel_key = hotel['hotelKey']

                # Get hotel details
                hotel_details = get_hotel_details(session_id, conversation_id, hotel_key, search_key)
                if hotel_details and 'data' in hotel_details:
                    property_info = hotel_details['data'][0]
                    save_property_info_to_json(property_info, provider_hotel_id, directory)

            # Update the contentUpdateStatus to 'OK'
            update_query = update(global_hotel_mapping).where(
                (global_hotel_mapping.c.CityName == city_name) &
                (global_hotel_mapping.c.CountryName == country_name)
            ).values(contentUpdateStatus='OK')
            session.execute(update_query)
            session.commit()
        else:
            # print(f"Error: 'data' key not found in hotel response for {city_name}, {country_name}")
            # Update the contentUpdateStatus to 'NOT OK'
            update_query = update(global_hotel_mapping).where(
                (global_hotel_mapping.c.CityName == city_name) &
                (global_hotel_mapping.c.CountryName == country_name)
            ).values(contentUpdateStatus='NOT OK 2')
            session.execute(update_query)
            session.commit()
    except SQLAlchemyError as e:
        # print(f"Error processing {city_name}, {country_name}: {e}")
        session.rollback()  # Rollback the transaction in case of error
        # Update the contentUpdateStatus to 'NOT OK'
        update_query = update(global_hotel_mapping).where(
            (global_hotel_mapping.c.CityName == city_name) &
            (global_hotel_mapping.c.CountryName == country_name)
        ).values(contentUpdateStatus='NOT OK 2')
        session.execute(update_query)
        session.commit()
    except Exception as e:
        # print(f"Error processing {city_name}, {country_name}: {e}")
        # Update the contentUpdateStatus to 'NOT OK'
        update_query = update(global_hotel_mapping).where(
            (global_hotel_mapping.c.CityName == city_name) &
            (global_hotel_mapping.c.CountryName == country_name)
        ).values(contentUpdateStatus='NOT OK 2')
        session.execute(update_query)
        session.commit()
    finally:
        Session.remove()  # Remove the session to avoid concurrency issues

def extract_provider_hotel_ids():
    auth_response = get_user_auth()
    if not auth_response:
        # print("Authentication failed. Exiting.")
        return

    session_id = auth_response['data'][0]['sessionId']
    conversation_id = auth_response['meta'].get('conversationId', '')
    if not conversation_id:
        conversation_id = str(uuid.uuid4())

    # Fetch records from the database
    query = select(global_hotel_mapping.c.CityName, global_hotel_mapping.c.CountryName, global_hotel_mapping.c.contentUpdateStatus).where(
        or_(
            global_hotel_mapping.c.contentUpdateStatus == 'NOT OK',
        )
    )
    result = Session.execute(query).fetchall()

    # Print the query result for debugging
    # print("Query Result:", result)

    # Fetch a few sample records to verify data
    sample_query = select(global_hotel_mapping).limit(5)
    sample_result = Session.execute(sample_query).fetchall()
    # print("Sample Records:", sample_result)

    if not result:
        print("No records found to process.")
        return

    directory = r"D:\content_for_hotel_json\HotelInfo\dnata"

    # Use ThreadPoolExecutor to process cities concurrently
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_city = {executor.submit(process_city, row[0], row[1], session_id, conversation_id, directory): (row[0], row[1]) for row in result}

        for future in as_completed(future_to_city):
            city_name, country_name = future_to_city[future]
            try:
                future.result()  # Get the result to raise any exceptions
            except Exception as exc:
                print(f"Error processing {city_name}, {country_name}: {exc}")

    Session.remove() 

# Call the function to extract provider hotel IDs and save property info to JSON files
extract_provider_hotel_ids()
