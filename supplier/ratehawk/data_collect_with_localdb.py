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

# Output configuration: JSON files will be saved in this directory
BASE_OUTPUT_DIR = "D:/content_for_hotel_json/cdn_row_collection/ratehawk"
TRACKING_FILE = os.path.join(BASE_OUTPUT_DIR, "processed_hotels.txt")
os.makedirs(BASE_OUTPUT_DIR, exist_ok=True)

# def clean_html(text):
#     """Remove simple HTML tags from text"""
#     return html.unescape(text).replace('<p>', '').replace('</p>', '') if text else ''

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
            `id` AS hotel_code,
            `address`,
            `amenity_groups`,
            `check_in_time`,
            `check_out_time`,
            `description_struct`,
            `images`,
            `images_ext`,
            `kind`,
            `latitude`,
            `longitude`,
            `name`,
            `phone`,
            `postal_code`,
            `region`,
            `star_rating`,
            `email`,
            `serp_filters`,
            `hotel_chain`
        FROM ratehawk_2
        WHERE `id` IS NOT NULL  # Skip entries with missing hid
    """)
    try:
        with engine.connect() as conn:
            result = conn.execute(query)
            return [dict(row._mapping) for row in result]
    except Exception as e:
        print(f"Error fetching data: {e}")
        return []

def convert_all_fields_to_string(hotel_data):
    """Convert all field values in hotel_data to strings"""
    for key, value in hotel_data.items():
        hotel_data[key] = str(value) if value is not None else ""
    return hotel_data

def save_hotel_json(hotel_data):
    try:
        hotel_code = str(hotel_data.get('hotel_code', '')).strip()
        
        # Validate hotel_code
        if not hotel_code or hotel_code.lower() in ('none', 'null', 'undefined', ''):
            print(f"Skipping invalid hotel_code: {hotel_code}")
            return False

        # Convert all fields to strings
        hotel_data = {k: str(v) if v is not None else "" for k, v in hotel_data.items()}

        filename = f"{hotel_code}.json"
        filepath = os.path.join(BASE_OUTPUT_DIR, filename)

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
        print(f"Error saving {hotel_code}: {str(e)}")
        return False

def update_tracking(hotel_code):
    hotel_code = str(hotel_code).strip()
    if not hotel_code or hotel_code.lower() in ('none', 'null', ''):
        return False
    try:
        with open(TRACKING_FILE, 'a') as f:
            f.write(f"{hotel_code}\n")
        return True
    except Exception as e:
        print(f"Tracking failed for {hotel_code}: {e}")
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

