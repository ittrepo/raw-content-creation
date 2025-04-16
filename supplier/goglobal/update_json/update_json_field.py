import os
import json
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Load environment variables from .env file
load_dotenv()

# Get database credentials from environment variables
db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')

# Table names
table_1 = "goglobal_master"
table_2 = "global_hotel_mapping"

# Directory containing JSON files
json_dir = r'D:\content_for_hotel_json\goglobal'

# Database connection setup
DATABASE_URL = f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

def get_giata_code(session, hotel_code):
    sql = text(f"SELECT giataCode FROM {table_1} WHERE hotelCode = :hotel_code")
    result = session.execute(sql, {'hotel_code': hotel_code}).fetchone()
    return result[0] if result else None

def get_hotel_data(session, giata_code):
    sql = text(f"""
    SELECT BrandName, PropertyType, Rating, ChainName, StateName, PostalCode, Latitude, Longitude, CityName, CountryName, CountryCode
    FROM {table_2}
    WHERE GiataCode = :giata_code
    """)
    result = session.execute(sql, {'giata_code': giata_code}).fetchone()
    return result if result else None

def update_json_file(file_path, hotel_data):
    with open(file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)

    if hotel_data:
        brand_text, property_type, star_rating, chain, state, postal_code, latitude, longitude, city, country, country_code = hotel_data
        brand = brand_text  # Assuming brand is the same as brand_text

        if data['brand_text'] is None:
            data['brand_text'] = brand_text
        if data['property_type'] is None:
            data['property_type'] = property_type
        if data['star_rating'] is None:
            data['star_rating'] = star_rating
        if data['chain'] is None:
            data['chain'] = chain
        if data['brand'] is None:
            data['brand'] = brand
        if data['address']['state'] is None:
            data['address']['state'] = state
        if data['address']['latitude'] is None:
            data['address']['latitude'] = latitude
        if data['address']['longitude'] is None:
            data['address']['longitude'] = longitude
        if data['address']['city'] is None:
            data['address']['city'] = city
        if data['address']['country'] is None:
            data['address']['country'] = country
        if data['address']['country_code'] is None:
            data['address']['country_code'] = country_code
        if data['address']['postal_code'] is None:
            data['address']['postal_code'] = postal_code
        if data['address']['local_lang']['postal_code'] is None:
            data['address']['local_lang']['postal_code'] = postal_code
        if data['address']['local_lang']['state'] is None:
            data['address']['local_lang']['state'] = state
        if data['address']['local_lang']['latitude'] is None:
            data['address']['local_lang']['latitude'] = latitude
        if data['address']['local_lang']['longitude'] is None:
            data['address']['local_lang']['longitude'] = longitude
        if data['address']['local_lang']['city'] is None:
            data['address']['local_lang']['city'] = city
        if data['address']['local_lang']['country'] is None:
            data['address']['local_lang']['country'] = country
        if data['address']['local_lang']['country_code'] is None:
            data['address']['local_lang']['country_code'] = country_code

    with open(file_path, 'w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False, indent=4)

def process_json_files():
    session = Session()
    try:
        for filename in os.listdir(json_dir):
            if filename.endswith('.json'):
                file_path = os.path.join(json_dir, filename)
                hotel_code = os.path.splitext(filename)[0]
                giata_code = get_giata_code(session, hotel_code)
                if giata_code:
                    hotel_data = get_hotel_data(session, giata_code)
                    update_json_file(file_path, hotel_data)
                    print(f"Updated {filename} with giataCode: {giata_code}")
                else:
                    print(f"Skipping {filename}: giataCode not found.")
    finally:
        session.close()

# Run the process
process_json_files()
