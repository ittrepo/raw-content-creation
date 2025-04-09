import requests
import json
from sqlalchemy import create_engine, Column, Integer, String, Float, Text
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import IntegrityError
import time

# Database configuration
DATABASE_URL = "mysql+pymysql://root:@localhost/csvdata01_02102024"
engine = create_engine(DATABASE_URL)
Base = declarative_base()

# Define the ORM model
class Hotel(Base):
    __tablename__ = 'oryx_systemid_master'
    id = Column(Integer, primary_key=True, autoincrement=True)
    giDestinationId = Column(String(255))
    destinationId = Column(String(255))
    name = Column(String(255))
    systemId = Column(String(255), unique=True) 
    rating = Column(Integer)
    city = Column(String(255))
    address1 = Column(String(255))
    address2 = Column(String(255))
    imageUrl = Column(String(255))
    lat = Column(Float)
    lon = Column(Float)
    mappings = Column(Text)

# Create the table if it doesn't exist
Base.metadata.create_all(engine)

# Function to fetch data from the API
def fetch_hotel_data(destination_id):
    url = "http://uat-apiv2.giinfotech.ae/api/v2/hotel/HotelsInfoByDestinationId"
    payload = json.dumps({"destinationId": destination_id})
    headers = {
        'Content-Type': 'application/json',
        'ApiKey': 'MtG5lGOBy06CpnY43AoGsA=='
    }
    response = requests.post(url, headers=headers, data=payload, timeout=10)

    # Check if the response is successful
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: Received status code {response.status_code} for destination ID {destination_id}")
        return None

# Function to save data to the database
def save_to_database(data, destination_id):
    if data is None:
        print("Error: No data to save.")
        return

    Session = sessionmaker(bind=engine)
    session = Session()

    hotels_information = data.get('hotelsInformation')
    if hotels_information is None:
        print("Error: 'hotelsInformation' not found in the response.")
        return

    for hotel_info in hotels_information:
        system_id = hotel_info.get('systemId')

        # Check if the systemId already exists in the database
        if session.query(Hotel).filter_by(systemId=system_id).first() is not None:
            print(f"Skipping entry with existing systemId: {system_id}")
            continue

        hotel = Hotel(
            giDestinationId=hotel_info.get('giDestinationId'),
            destinationId=hotel_info.get('destinationId'),
            name=hotel_info.get('name'),
            systemId=system_id,
            rating=hotel_info.get('rating'),
            city=hotel_info.get('city'),
            address1=hotel_info.get('address1'),
            address2=hotel_info.get('address2'),
            imageUrl=hotel_info.get('imageUrl'),
            lat=hotel_info.get('geoCode', {}).get('lat'),
            lon=hotel_info.get('geoCode', {}).get('lon'),
            mappings=json.dumps(hotel_info.get('mappings'))
        )
        session.add(hotel)

    try:
        session.commit()
        print(f"Data saved successfully for destination ID: {destination_id}")
        return True
    except IntegrityError:
        session.rollback()
        print(f"Error: IntegrityError occurred for destination ID {destination_id}")
        return False
    finally:
        session.close()

# Function to read destination IDs from the file
def read_destination_ids(file_path):
    with open(file_path, 'r') as file:
        return [line.strip() for line in file.readlines()]

# Function to write destination IDs to the file
def write_destination_ids(file_path, destination_ids):
    with open(file_path, 'w') as file:
        file.write("\n".join(destination_ids))

# Main processing loop
file_path = 'get_all_destination_id.txt'

while True:
    destination_ids = read_destination_ids(file_path)

    if not destination_ids:
        print("All destination IDs have been processed.")
        break

    for destination_id in destination_ids[:]:  # Iterate over a copy of the list
        data = fetch_hotel_data(destination_id)
        if save_to_database(data, destination_id):
            destination_ids.remove(destination_id)
            write_destination_ids(file_path, destination_ids)

    # Optional: Add a delay to avoid rapid consecutive API requests
    time.sleep(1)
