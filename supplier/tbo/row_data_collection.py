import os
import json
import requests
import base64
from dotenv import load_dotenv

def load_credentials():
    """Load credentials from environment variables."""
    load_dotenv()
    return {
        'user_name': os.getenv('TOB_USERNAME', '').strip(),
        'user_password': os.getenv('TOB_PASSWORD', '').strip(),
        'base_url': os.getenv('TOB_BASE_URL', '').strip()
    }

def hotel_api_authentication(credentials):
    """Generate authentication details."""
    try:
        authorization_basic = base64.b64encode(f"{credentials['user_name']}:{credentials['user_password']}".encode()).decode()
        return {
            'Authorization': f"Basic {authorization_basic}",
            'Content-Type': 'application/json'
        }
    except Exception as e:
        print(f"Error in hotel API authentication: {e}")
        return None

def get_hotel_details(hotel_id):
    """Fetch hotel details from the API."""
    try:
        print(hotel_id)
        credentials = load_credentials()
        headers = hotel_api_authentication(credentials)
        base_url = credentials['base_url']
        
        if not headers or not base_url:
            print("Missing authentication details.")
            return None
        
        payload = {
            "Hotelcodes": hotel_id,
            "Language": "en"
        }
        
        response = requests.post(f"{base_url}/Hoteldetails", headers=headers, json=payload)
        
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            print(f"Failed to fetch hotel details: {response.text}")
            return None
    except Exception as e:
        print(f"Error in fetching hotel details: {e}")
        return None

def save_json(raw_path, hotel_id):
    json_file_path = os.path.join(raw_path, f"{hotel_id}.json")
    hotel_data = get_hotel_details(hotel_id)
    
    if hotel_data is None:
        print(f"Warning: {hotel_id}.json - Data fetch failed. Saving default empty JSON.")
        hotel_data = {}
    
    with open(json_file_path, "w", encoding="utf-8") as file:
        json.dump(hotel_data, file, indent=4)
    
    print(f"{hotel_id}.json saved successfully at {json_file_path}")













HOTEL_ID_LIST = "D:/Rokon/ofc_git/row_content_create/hotel_id_count_function/tbo/tbohotel_hotel_id_list.txt"
TRACKING_FILE = "D:/Rokon/ofc_git/row_content_create/hotel_id_count_function/tbo/tbohotel_tracking_file.txt"
NOT_FOUND_FILE = "D:/Rokon/ofc_git/row_content_create/hotel_id_count_function/tbo/tbohotel_not_found.txt"

def initialize_tracking_file(file_path, hotel_id_list):
    """Initializes the tracking file with all hotel IDs if it doesn't exist."""
    if not os.path.exists(file_path):
        with open(file_path, "w", encoding="utf-8") as file:
            file.write("\n".join(map(str, hotel_id_list)) + "\n")
    else:
        print(f"Tracking file already exists: {file_path}")

def read_tracking_file(file_path):
    """Reads the tracking file and returns a list of remaining hotel IDs."""
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as file:
            return [line.strip() for line in file if line.strip()]
    return []

def write_tracking_file(file_path, remaining_ids):
    """Updates the tracking file with unprocessed hotel IDs."""
    try:
        with open(file_path, "w", encoding="utf-8") as file:
            file.write("\n".join(remaining_ids) + "\n")
    except Exception as e:
        print(f"Error writing to tracking file: {e}")

def append_to_not_found_file(file_path, hotel_id):
    """Appends the hotel ID to the not found file."""
    try:
        with open(file_path, "a", encoding="utf-8") as file:
            file.write(hotel_id + "\n")
    except Exception as e:
        print(f"Error writing to not found file: {e}")


def process_hotels():
    if not os.path.exists(HOTEL_ID_LIST):
        print(f"Hotel ID list file not found: {HOTEL_ID_LIST}")
        return

    with open(HOTEL_ID_LIST, "r", encoding="utf-8") as file:
        all_hotel_ids = [line.strip() for line in file if line.strip()]

    initialize_tracking_file(TRACKING_FILE, all_hotel_ids)
    hotel_ids_to_process = read_tracking_file(TRACKING_FILE)

    if not hotel_ids_to_process:
        print("No hotel IDs left to process.")
        return



    supplier = "tbo"
    base_path = f"D:/content_for_hotel_json/cdn_row_collection/{supplier}"

    for hotel_id in hotel_ids_to_process:
        try:
            print(f"Processing hotel ID: {hotel_id}")
            save_json(base_path, hotel_id)
        except Exception as e:
            print(f"Error processing hotel ID {hotel_id}: {e}")
            append_to_not_found_file(NOT_FOUND_FILE, hotel_id)
            continue 

        hotel_ids_to_process.remove(hotel_id)
        write_tracking_file(TRACKING_FILE, hotel_ids_to_process)

    print("Processing completed.")

# Run the function
process_hotels()



