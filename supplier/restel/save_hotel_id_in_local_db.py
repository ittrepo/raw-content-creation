import urllib.parse
import requests
import xml.etree.ElementTree as ET
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pandas as pd

# --- API Request Part ---
xml_data = """<?xml version="1.0" encoding="UTF-8"?>
<peticion>
  <tipo>17</tipo>
  <nombre>Servicio de listado de hoteles</nombre>
  <agencia>Agencia prueba</agencia>
</peticion>"""

encoded_xml = urllib.parse.quote(xml_data)
url = f"http://xml.hotelresb2b.com/xml/listen_xml.jsp?codigousu=ZVYE&clausu=xml514142&afiliacio=RS&secacc=151003&xml={encoded_xml}"
headers = {'Cookie': 'JSESSIONID=aaaodjlEZaLhM_vAad2xz'}

# Make the API request
response = requests.get(url, headers=headers)

# Check if request was successful
if response.status_code != 200:
    raise Exception(f"API request failed with status code {response.status_code}")

# --- XML Parsing Part ---
try:
    root = ET.fromstring(response.text)
except ET.ParseError as e:
    print("Failed to parse XML response:")
    print(response.text)
    raise

# Extract hotel data
hotels = []
for hotel in root.findall('.//hotel'):
    hotel_data = {
        'hotel_located': hotel.findtext('codesthot', default='').strip(),
        'city_code': hotel.findtext('codpobhot', default='').strip(),
        'internal_use': hotel.findtext('hot_codigo', default='').strip(),
        'hotel_code': hotel.findtext('hot_codcobol', default='').strip(),
        'hotel_affiliation': hotel.findtext('hot_afiliacion', default='').strip()
    }
    hotels.append(hotel_data)

# --- Database Part ---
DATABASE_URL = "mysql+pymysql://root:@localhost/csvdata01_02102024"
engine = create_engine(DATABASE_URL)

Base = declarative_base()

class RestelHotelDestination(Base):
    __tablename__ = 'restel_hotel_destination_2'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    hotel_located = Column(String(255))
    city_code = Column(String(255))
    internal_use = Column(String(255))
    hotel_code = Column(String(255))
    hotel_affiliation = Column(String(255))

# Create table if not exists
Base.metadata.create_all(engine)

# Insert data
if hotels:
    df = pd.DataFrame(hotels)
    df.to_sql(
        name='restel_hotel_destination_2',
        con=engine,
        if_exists='append',
        index=False,
        chunksize=1000
    )
    print(f"Successfully inserted {len(df)} records")
else:
    print("No hotel data found in the XML response")

print("Process completed successfully")