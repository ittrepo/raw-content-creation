import requests
import json
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os

load_dotenv()

hyperguest_token = os.getenv("HYPERGUEST_TOKEN")

DATABASE_URL = "mysql+pymysql://root:@localhost/csvdata01_02102024"
engine = create_engine(DATABASE_URL)
Base = declarative_base()

class Hotel(Base):
    __tablename__ = 'hypergutesdirect_hotel_master'
    id = Column(Integer, primary_key=True, autoincrement=True)
    hotel_id = Column(String(100))
    name = Column(String(255))
    country_code = Column(String(5))
    region = Column(String(30))
    city = Column(String(30))
    city_id = Column(String(30))

Base.metadata.create_all(engine)

def fetch_hotel_information():
    url = "https://hg-static.hyperguest.com/hotels.json"

    headers = {
        'Authorization': f'Bearer {hyperguest_token}'
    }

    response = requests.get(url, headers=headers, timeout=50)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: Received status code {response.status_code}")
        return None

def save_to_database(data):
    if data is None:
        print("Error: No data to save.")
        return

    Session = sessionmaker(bind=engine)
    session = Session()

    for hotel in data:
        hotel_id = hotel.get('hotel_id')
        name = hotel.get('name')
        country_code = hotel.get('country_code')
        region = hotel.get('region')
        city = hotel.get('city')
        city_id = hotel.get('city_id')

        # Check if the hotel already exists in the database
        existing_hotel = session.query(Hotel).filter_by(hotel_id=hotel_id).first()
        if existing_hotel:
            print(f"Hotel {hotel_id} already exists in the database. Skipping...")
            continue

        new_hotel = Hotel(
            hotel_id=hotel_id,
            name=name,
            country_code=country_code,
            region=region,
            city=city,
            city_id=city_id
        )
        session.add(new_hotel)

    try:
        session.commit()
        print(f"Successfully saved {len(data)} records to the database.")
    except Exception as e:
        session.rollback()
        print(f"Error saving data to the database: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    data = fetch_hotel_information()
    if data:
        save_to_database(data)
