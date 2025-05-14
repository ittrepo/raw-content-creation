import requests
import json
import os
from sqlalchemy import create_engine, MetaData, Table, select, update
from sqlalchemy.orm import scoped_session, sessionmaker
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database configuration
DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')
AKBAR_API_KEY = os.getenv("AKBAR_APIKEY")
AKBAR_CLIENT_ID = os.getenv("AKBAR_CLIENT_ID")

DATABASE_URI = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}"

# API configuration
BASE_API_URL = "https://travelportalapi.benzyinfotech.com"
LOGIN_URL = "https://b2bapiutils.benzyinfotech.com/Utils/Signature"
SEARCH_INIT_URL = f"{BASE_API_URL}/api/hotels/search/init"
SEARCH_RESULT_URL = f"{BASE_API_URL}/api/hotels/search/result"

# Output directory
OUTPUT_DIR = r"D:\content_for_hotel_json\cdn_row_collection\akbar"
def get_auth_headers():
    """Get authentication headers by logging in once"""
    login_payload = {
        "MerchantID": "300",
        "ApiKey": AKBAR_API_KEY,
        "ClientID": AKBAR_CLIENT_ID,
        "Password": "staging@1",
        "AgentCode": "",
        "BrowserKey": "caecd3cd30225512c1811070dce615c1"
    }
    
    response = requests.post(LOGIN_URL, json=login_payload)
    if response.status_code != 200:
        raise Exception("Authentication failed")
    
    auth_data = response.json()
    return {
        "Authorization": f"Bearer {auth_data['Token']}",
        "user-session-key": auth_data["TUI"]
    }

def get_locations():
    """Fetch locations from database"""
    engine = create_engine(DATABASE_URI)
    metadata = MetaData()
    locations_table = Table('akbar_location_master', metadata, 
                          autoload_with=engine)
    
    session_factory = sessionmaker(bind=engine)
    Session = scoped_session(session_factory)
    session = Session()
    
    try:
        query = select(
            locations_table.c.id,
            locations_table.c.lat,
            locations_table.c.long,
            locations_table.c.country
        ).where(locations_table.c.status.is_(None))  
        
        result = session.execute(query)
        return [{
            'id': row.id,
            'lat': row.lat,
            'long': row.long,
            'country': row.country
        } for row in result]
    finally:
        session.close()
        engine.dispose()

def update_location_status(location_id):
    """Update location status to 'OK' in database using ID"""
    engine = create_engine(DATABASE_URI)
    metadata = MetaData()
    locations_table = Table('akbar_location_master', metadata, 
                          autoload_with=engine)
    
    session_factory = sessionmaker(bind=engine)
    session = session_factory()
    
    try:
        update_stmt = (
            update(locations_table)
            .where(locations_table.c.id == location_id)
            .values(status='OK')
        )
        
        result = session.execute(update_stmt)
        session.commit()
        print(f"Updated status for location ID: {location_id}")
    except Exception as e:
        session.rollback()
        print(f"Failed to update status for ID {location_id}: {str(e)}")
    finally:
        session.close()
        engine.dispose()

def search_hotels(auth_headers, location):
    """Perform hotel search for a location"""
    # Prepare search init payload
    search_init_payload = {
        "geoCode": {
            "lat": str(location['lat']),
            "long": str(location['long'])
        },
        "destinationCountryCode": location['country'].upper(),
        "currency": "INR",
        "culture": "en-US",
        "checkIn": "05/20/2025",
        "checkOut": "05/22/2025",
        "rooms": [{"adults": "2", "children": "0", "childAges": []}],
        "agentCode": "",
        "nationality": "IN",
        "countryOfResidence": "IN",
        "channelId": "b2bsaudideals",
        "affiliateRegion": "B2B_India",
        "segmentId": "",
        "companyId": "1",
        "gstPercentage": 0,
        "tdsPercentage": 0
    }

    # Send search init request
    init_response = requests.post(
        SEARCH_INIT_URL,
        headers=auth_headers,
        json=search_init_payload
    )

    if init_response.status_code != 200:
        print(f"Search init failed for {location}: {init_response.text}")
        return None

    init_data = init_response.json()
    if init_data.get('status') != 'success':
        print(f"Search init error for {location}: {init_data}")
        return None

    # Get search results
    search_id = init_data['searchId']
    tracing_key = init_data['searchTracingKey']

    result_url = f"{SEARCH_RESULT_URL}/{search_id}/content?limit=200&offset=0"
    result_headers = {
        **auth_headers,
        "search-tracing-key": tracing_key
    }

    result_response = requests.get(result_url, headers=result_headers)
    if result_response.status_code != 200:
        print(f"Search result failed for {location}: {result_response.text}")
        return None

    return result_response.json()

def save_hotels(hotels_data):
    """Save hotel data to JSON files with existence check"""
    if not hotels_data or not hotels_data.get('hotels'):
        print("No hotels found in response")
        return 0
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    saved_count = 0
    
    for hotel in hotels_data['hotels']:
        hotel_id = hotel.get('id')
        if not hotel_id:
            continue
            
        file_path = os.path.join(OUTPUT_DIR, f"{hotel_id}.json")
        
        # Skip existing files
        if os.path.exists(file_path):
            print(f"File exists: {hotel_id}.json")
            continue
            
        with open(file_path, 'w') as f:
            json.dump(hotel, f, indent=2)
        print(f"🐍🐍🐍🐍🐍Saved hotel {hotel_id}")
        saved_count += 1
    
    return saved_count

def main():
    try:
        auth_headers = get_auth_headers()
        locations = get_locations()
        print(f"Found {len(locations)} locations to process")
        
        for location in locations:
            location_id = location['id']
            print(f"\nProcessing location ID: {location_id}")
            
            # Skip if any hotel files already exist for this location
            # (Add this check if needed)
            
            hotels_data = search_hotels(auth_headers, location)
            if not hotels_data:
                continue
                
            saved_count = save_hotels(hotels_data)
            
            # Only update status if we processed successfully
            if saved_count > 0:
                update_location_status(location_id)
            else:
                print("No new hotels saved, status remains unchanged")
                
    except Exception as e:
        print(f"Error in main execution: {str(e)}")

if __name__ == "__main__":
    main()