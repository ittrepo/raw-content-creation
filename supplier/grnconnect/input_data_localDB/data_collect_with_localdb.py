import json
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import html
import time


# Load environment variables
load_dotenv()

# Database configuration
DATABASE_URL = "mysql+pymysql://root:@localhost/csvdata01_02102024"
engine = create_engine(DATABASE_URL)

# Output configuration
BASE_OUTPUT_DIR = "D:/content_for_hotel_json/cdn_row_collection/grnconnect"
TRACKING_FILE = os.path.join(BASE_OUTPUT_DIR, "processed_hotels.txt")
os.makedirs(BASE_OUTPUT_DIR, exist_ok=True)

def clean_html(text):
    """Remove simple HTML tags from text"""
    return html.unescape(text).replace('<p>', '').replace('</p>', '') if text else ''

def get_db_connection():
    """Create and return a database connection"""
    try:
        return engine.connect()
    except Exception as e:
        print(f"Database connection failed: {e}")
        return None

def fetch_hotel_data():
    """Fetch all hotel data from database"""
    query = text("""
        SELECT DISTINCT 
            `Hotel Code` AS hotel_code,
            `Hotel Name` AS hotel_name,
            `Description` AS description,
            `City Code` AS city_code,
            `Country Code` AS country_code,
            `Address` AS address,
            `Postal Code` AS postal_code,
            `Latitude` AS latitude,
            `Longitude` AS longitude,
            `Star Category` AS star_rating,
            `Accommodation Type` AS accommodation_type,
            `Accommodation Type Sub Name` AS accommodation_type_sub_name,
            `Chain Name` AS chain_name
        FROM grn_master
    """)
    
    try:
        with engine.connect() as conn:
            result = conn.execute(query)
            return [dict(row._mapping) for row in result]
    except Exception as e:
        print(f"Error fetching data: {e}")
        return []

def convert_types(hotel_data):
    """Convert numeric fields to appropriate types"""
    conversions = {
        'hotel_code': int,
        'city_code': int,
        'latitude': float,
        'longitude': float,
        'star_rating': float,
        'accommodation_type': int
    }
    
    for key, conv_func in conversions.items():
        if hotel_data.get(key) is not None:
            try:
                hotel_data[key] = conv_func(hotel_data[key])
            except (ValueError, TypeError):
                hotel_data[key] = None
    return hotel_data

def save_hotel_json(hotel_data):
    """Save individual hotel data to JSON file"""
    try:
        hotel_code = str(hotel_data['hotel_code'])
        filename = f"{hotel_code}.json"
        filepath = os.path.join(BASE_OUTPUT_DIR, filename)
        
        # Clean and transform data
        hotel_data['description'] = clean_html(hotel_data.get('description', ''))
        hotel_data = convert_types(hotel_data)
        
        # Create JSON structure
        json_output = {
            "metadata": {
                "hotel_id": hotel_code,
                "source": "local_database",
                "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            },
            "hotel_info": hotel_data
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(json_output, f, indent=2, ensure_ascii=False)
            
        return True
    except Exception as e:
        print(f"Error processing {hotel_code}: {e}")
        return False

def update_tracking(hotel_code):
    """Update tracking file with processed hotel codes"""
    try:
        with open(TRACKING_FILE, 'a') as f:
            f.write(f"{hotel_code}\n")
        return True
    except Exception as e:
        print(f"Tracking update failed: {e}")
        return False

def process_hotels():
    """Main processing function"""
    # Check existing processed hotels
    processed = set()
    if os.path.exists(TRACKING_FILE):
        with open(TRACKING_FILE, 'r') as f:
            processed = set(line.strip() for line in f if line.strip())
    
    # Fetch data from database
    hotels = fetch_hotel_data()
    if not hotels:
        print("No hotel data found in database")
        return
    
    total = len(hotels)
    success_count = 0
    print(f"Starting processing of {total} hotels...")
    
    for idx, hotel in enumerate(hotels, 1):
        hotel_code = str(hotel.get('hotel_code', 'unknown'))
        
        if hotel_code in processed:
            print(f"[{idx}/{total}] Skipping {hotel_code} - Already processed")
            continue
            
        print(f"[{idx}/{total}] Processing {hotel_code}...")
        
        if save_hotel_json(hotel):
            if update_tracking(hotel_code):
                success_count += 1
            else:
                print(f"Warning: Failed to track {hotel_code}")
    
    print(f"\nProcessing complete! Success: {success_count}/{total}")

if __name__ == "__main__":
    process_hotels()