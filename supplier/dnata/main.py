import requests
import json
import uuid
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table, select, update
from sqlalchemy.orm import sessionmaker

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
global_hotel_mapping = Table("global_hotel_mapping_copy", metadata, autoload_with=engine)

# Create a session
Session = sessionmaker(bind=engine)
session = Session()

def get_user_auth():
    url = "https://gmtmsuat.provesio.com//api/v1/auth/login"

    payload = json.dumps({
    "userName": "demo@gm.ae",
    "password": "Varank@2710",
    "companyCode": "CE000017"
    })
    headers = {
    'Content-Type': 'application/json',
    'X-API-KEY': '7453cc1d-af64-4259-bfca-cf36e167e134'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    return response.json()

def get_hotel_id(session_id, conversation_id, country, city):
    url = "https://gmtmsuat.provesio.com//api/v1/hotel/search"

    payload = json.dumps({
    "country": country,
    "city": city,
    "checkIn": "2025-06-11",
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

    return response.json()

def save_property_info_to_json(property_info, provider_hotel_id, directory):
    # Ensure the directory exists
    os.makedirs(directory, exist_ok=True)

    # Create the file path
    file_path = os.path.join(directory, f"{provider_hotel_id}.json")

    # Write the property_info to the file
    with open(file_path, 'w') as file:
        json.dump(property_info, file, indent=4)

    print(f"Saved {file_path}")

def extract_provider_hotel_ids():
    auth_response = get_user_auth()
    # print("Auth Response:", auth_response)  # Debugging statement

    session_id = auth_response['data'][0]['sessionId']
    conversation_id = auth_response['meta'].get('conversationId', '')

    # Generate a new UUID for conversationId if it is empty
    if not conversation_id:
        conversation_id = str(uuid.uuid4())
        # print(f"Generated new conversationId: {conversation_id}")

    # Fetch records from the database
    query = select(global_hotel_mapping.c.CityName, global_hotel_mapping.c.CountryName, global_hotel_mapping.c.contentUpdateStatus).where(global_hotel_mapping.c.contentUpdateStatus != 'OK')
    result = session.execute(query).fetchall()

    directory = r"D:\content_for_hotel_json\HotelInfo\dnata"

    for row in result:
        city_name = row[0]  # Access by index
        country_name = row[1]  # Access by index

        try:
            hotel_response = get_hotel_id(session_id, conversation_id, country_name, city_name)
            # print("Hotel Response:", hotel_response)  # Debugging statement

            if 'data' in hotel_response:
                for hotel in hotel_response['data']:
                    provider_hotel_id = hotel['propertyInfo']['providerHotelId']
                    property_info = hotel['propertyInfo']
                    save_property_info_to_json(property_info, provider_hotel_id, directory)

                # Update the contentUpdateStatus to 'OK'
                update_query = update(global_hotel_mapping).where(
                    (global_hotel_mapping.c.CityName == city_name) &
                    (global_hotel_mapping.c.CountryName == country_name)
                ).values(contentUpdateStatus='OK')
                session.execute(update_query)
                session.commit()
            else:
                print(f"Error: 'data' key not found in hotel response for {city_name}, {country_name}")
                # Update the contentUpdateStatus to 'NOT OK'
                update_query = update(global_hotel_mapping).where(
                    (global_hotel_mapping.c.CityName == city_name) &
                    (global_hotel_mapping.c.CountryName == country_name)
                ).values(contentUpdateStatus='NOT OK')
                session.execute(update_query)
                session.commit()
        except Exception as e:
            print(f"Error processing {city_name}, {country_name}: {e}")
            # Update the contentUpdateStatus to 'NOT OK'
            update_query = update(global_hotel_mapping).where(
                (global_hotel_mapping.c.CityName == city_name) &
                (global_hotel_mapping.c.CountryName == country_name)
            ).values(contentUpdateStatus='NOT OK')
            session.execute(update_query)
            session.commit()

    session.close()

# Call the function to extract provider hotel IDs and save property info to JSON files
extract_provider_hotel_ids()
