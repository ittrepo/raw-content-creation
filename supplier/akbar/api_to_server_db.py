from sqlalchemy import create_engine, MetaData, Table, select, update, not_, or_
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.exc import SQLAlchemyError
import requests
import json
import os
from dotenv import load_dotenv
from datetime import datetime

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

# Check if the tables exist
if 'country_info' in metadata.tables and 'akbar_location_master' in metadata.tables:
    country_info = Table("country_info", metadata, autoload_with=engine)
    akbar_location_master = Table("akbar_location_master", metadata, autoload_with=engine)
else:
    print("Required tables do not exist in the database.")
    exit()

# Create a scoped session for thread-safe operations
session_factory = sessionmaker(bind=engine)
Session = scoped_session(session_factory)

akbar_api_key = os.getenv("AKBAR_APIKEY")
akbar_client_id = os.getenv("AKBAR_CLIENT_ID")
akbar_password = os.getenv("AKBAR_PASSWORD")
akbar_browser_key = os.getenv("AKBAR_BROWSER_KEY")

# Fetch the token
url = "https://b2bapiutils.benzyinfotech.com/Utils/Signature"
payload = json.dumps({
  "MerchantID": "300",
  "ApiKey": akbar_api_key,
  "ClientID": akbar_client_id,
  "Password": akbar_password,
  "AgentCode": "",
  "BrowserKey": akbar_browser_key
})
headers = {
  'Content-Type': 'application/json'
}

response = requests.post(url, headers=headers, data=payload)
token_data = response.json()
token = token_data.get('Token')

if not token:
    print("Failed to retrieve token.")
    exit()

# Fetch city names from the database
session = Session()
# print("hellow")
city_names = session.query(country_info.c.CityName).filter(
    or_(
        country_info.c.akbar_status != 'ok',
        country_info.c.akbar_status == None  # Handles NULL values
    )
).all()
# print(city_names)
for city_name in city_names:
    city_name = city_name[0]
    url = f"https://travelportal.benzyinfotech.com/api/content/autosuggest?term={city_name}"
    headers = {
      'Authorization': f'Bearer {token}'
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        # Update akbar_status to 'ok'
        session.execute(
            update(country_info).where(country_info.c.CityName == city_name).values(akbar_status='ok')
        )
        session.commit()  # Commit the update immediately

        # Insert data into akbar_location_master
        locations = response.json().get('locations', [])
        for location in locations:
            session.execute(akbar_location_master.insert().values(
                location_id=location['id'],
                name=location['name'],
                full_name=location['fullName'],
                code=location['code'],
                type=location['type'],
                city=location['city'],
                state=location['state'],
                country=location['country'],
                score=location['score'],
                lat=location['coordinates']['lat'],
                long=location['coordinates']['long'],
            ))
            session.commit()  # Commit each insertion immediately
            print(f"Inserted location: {location['name']}")
    else:
        # Update akbar_status to 'Not Ok'
        session.execute(
            update(country_info).where(country_info.c.CityName == city_name).values(akbar_status='Not Ok')
        )
        session.commit()  # Commit the update immediately

# Close the session
session.close()
