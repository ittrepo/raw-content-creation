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

metadata = MetaData()
metadata.reflect(bind=engine)

# Check if the table exists
if 'country_info' in metadata.tables:
    global_hotel_mapping = Table("country_info", metadata, autoload_with=engine)
else:
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
        return None

def get_hotel_id(session_id, conversation_id, country, city):
    url = "https://gmtmsuat.provesio.com//api/v1/hotel/search"
    payload = json.dumps({
        "country": country,
        "city": city,
        "checkIn": "2025-08-15",
        "checkOut": "2025-08-30",
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
        return None

def save_property_info_to_json(property_info, provider_hotel_id, directory):
    # Ensure the directory exists
    os.makedirs(directory, exist_ok=True)
    # Create the file path
    file_path = os.path.join(directory, f"{provider_hotel_id}.json")
    # Write the property_info to the file
    with open(file_path, 'w') as file:
        json.dump(property_info, file, indent=4)
    print(f"🐍🐍🐍🐍🐍🐍🐍 Saved {file_path}")

def process_city(city_name, country_name, session_id, conversation_id, directory):
    session = Session()
    try:
        hotel_response = get_hotel_id(session_id, conversation_id, country_name, city_name)
        if hotel_response and 'data' in hotel_response:
            search_key = hotel_response['commonData'].get('searchKey')
            if not search_key:
                print(f"Error: 'searchKey' not found for {city_name}, {country_name}")
                return

            processed = False  # Track if any hotel was processed
            for hotel in hotel_response['data']:
                provider_hotel_id = hotel['propertyInfo']['providerHotelId']
                hotel_key = hotel['hotelKey']

                # Check if JSON file already exists
                file_path = os.path.join(directory, f"{provider_hotel_id}.json")
                if os.path.exists(file_path):
                    print(f"Skipping {provider_hotel_id} as JSON file exists.")
                    continue

                # Get hotel details
                hotel_details = get_hotel_details(session_id, conversation_id, hotel_key, search_key)
                if hotel_details and 'data' in hotel_details:
                    property_info = hotel_details['data'][0]
                    save_property_info_to_json(property_info, provider_hotel_id, directory)
                    processed = True

            # Update the contentUpdateStatus to 'OK' if processed or all exist
            update_query = update(global_hotel_mapping).where(
                (global_hotel_mapping.c.CityName == city_name) &
                (global_hotel_mapping.c.CountryName == country_name)
            ).values(contentUpdateStatus='OK')
            session.execute(update_query)
            session.commit()
        else:
            # Update the contentUpdateStatus to 'NOT OK 3' if no data
            update_query = update(global_hotel_mapping).where(
                (global_hotel_mapping.c.CityName == city_name) &
                (global_hotel_mapping.c.CountryName == country_name)
            ).values(contentUpdateStatus='NOT OK 3')
            session.execute(update_query)
            session.commit()
    except SQLAlchemyError as e:
        session.rollback()
        update_query = update(global_hotel_mapping).where(
            (global_hotel_mapping.c.CityName == city_name) &
            (global_hotel_mapping.c.CountryName == country_name)
        ).values(contentUpdateStatus='NOT OK 3')
        session.execute(update_query)
        session.commit()
    except Exception as e:
        session.rollback()
        update_query = update(global_hotel_mapping).where(
            (global_hotel_mapping.c.CityName == city_name) &
            (global_hotel_mapping.c.CountryName == country_name)
        ).values(contentUpdateStatus='NOT OK 3')
        session.execute(update_query)
        session.commit()
    finally:
        Session.remove()

def extract_provider_hotel_ids():
    auth_response = get_user_auth()
    if not auth_response:
        return

    session_id = auth_response['data'][0]['sessionId']
    conversation_id = auth_response['meta'].get('conversationId', '')
    if not conversation_id:
        conversation_id = str(uuid.uuid4())

    # Fetch records where contentUpdateStatus is NULL
    query = select(global_hotel_mapping.c.CityName, global_hotel_mapping.c.CountryName).where(
        global_hotel_mapping.c.contentUpdateStatus == None
    )
    result = Session.execute(query).fetchall()

    if not result:
        print("No records found to process.")
        return

    directory = r"D:\content_for_hotel_json\HotelInfo\dnata"

    # Use ThreadPoolExecutor to process cities concurrently
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_city = {executor.submit(process_city, row[0], row[1], session_id, conversation_id, directory): (row[0], row[1]) for row in result}

        for future in as_completed(future_to_city):
            city_name, country_name = future_to_city[future]
            try:
                future.result()
            except Exception as exc:
                print(f"Error processing {city_name}, {country_name}: {exc}")

    Session.remove()

# Call the function to extract provider hotel IDs and save property info to JSON files
extract_provider_hotel_ids()