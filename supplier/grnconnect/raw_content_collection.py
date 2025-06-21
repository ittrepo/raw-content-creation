import os
import time
import hashlib
import requests
from dotenv import load_dotenv
import json

load_dotenv()

# Constants
HOTEL_ID_LIST = "D:/Rokon/ofc_git/row_content_create/hotel_id_count_function/grnconnect/grnconnect_hotel_id_list.txt"
TRACKING_FILE = "D:/Rokon/ofc_git/row_content_create/hotel_id_count_function/grnconnect/grnconnect_tracking_file.txt"
SUCCESS_FILE = "D:/Rokon/ofc_git/row_content_create/hotel_id_count_function/grnconnect/successful_done_hotel_id_list.txt"
NOT_FOUND_FILE = "D:/Rokon/ofc_git/row_content_create/hotel_id_count_function/grnconnect/grnconnect_not_found.txt"
BASE_PATH = "D:/content_for_hotel_json/cdn_row_collection/grnconnect_new"


# adjust as needed
API_KEY = os.getenv("GRNCONNECT_API_KEY")
HEADERS = {
    'Content-Type': 'application/json',
    'Accept': 'application/json',
    'Accept-Encoding': 'application/gzip',
    'api-key': API_KEY
}
CITY_HEADERS = {
    'Content-Type': 'application/json',
    'Accept': 'application/json',
    'API-key': API_KEY
}

def get_supplier_own_raw_data(hotel_id):
    """
    Fetch hotel, country, city and image info for the given hotel_id,
    save it as JSON to BASE_PATH/hotel_id.json (including hotel_code),
    and return the combined dict.
    """
    # ensure output directory exists
    os.makedirs(BASE_PATH, exist_ok=True)

    # 1) hotel info
    hotel_url = f"https://api-sandbox.grnconnect.com/api/v3/hotels?hcode={hotel_id}&version=2.0"
    resp = requests.get(hotel_url, headers=HEADERS)
    resp.raise_for_status()
    hotel_payload = resp.json()
    hotel = hotel_payload['hotels'][0]

    # 2) country info
    country_code = hotel.get('country')
    country_url = f"https://api-sandbox.grnconnect.com/api/v3/countries/{country_code}"
    resp = requests.get(country_url, headers=HEADERS)
    resp.raise_for_status()
    country = resp.json().get('country', {})

    # 3) city info
    city_code = hotel.get('city_code')
    city_url = f"https://api-sandbox.grnconnect.com/api/v3/cities/{city_code}?version=2.0"
    resp = requests.get(city_url, headers=CITY_HEADERS)
    resp.raise_for_status()
    city = resp.json().get('city', {})

    # 4) images
    images_url = (
        f"https://api-sandbox.grnconnect.com/api/v3/hotels/"
        f"{hotel_id}/images?version=2.0"
    )
    resp = requests.get(images_url, headers=HEADERS)
    resp.raise_for_status()
    images = resp.json().get('images', {}).get('regular', [])

    # combine into single dict, including hotel_code
    data = {
        'hotel_code': hotel_id,
        'hotel': hotel,
        'country': country,
        'city': city,
        'images': images
    }

    # write out to file
    out_path = os.path.join(BASE_PATH, f"{hotel_id}.json")
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    return data

def save_json(raw_path, hotel_id):
    """
    Save hotel data as a JSON file.
    """
    json_file_path = os.path.join(raw_path, f"{hotel_id}.json")
    data = get_supplier_own_raw_data(hotel_id)
    
    if data is None:
        print(f"Warning: {hotel_id}.json - Data fetch failed. Saving default empty JSON.")
        data = {}
    
    try:
        with open(json_file_path, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=4)
        print(f"🐍🐍🐍🐍🐍🐍🐍🐍{hotel_id}🐍🐍🐍.json saved successfully at 🐍🐍{json_file_path}")
        return True
    except Exception as e:
        print(f"❌❌Error saving JSON for hotel ❌❌{hotel_id}: {e}")
        return False

def initialize_tracking_file(file_path, hotel_id_list):
    """
    Initialize the tracking file with all hotel IDs if it doesn't exist.
    """
    if not os.path.exists(file_path):
        with open(file_path, "w", encoding="utf-8") as file:
            file.write("\n".join(map(str, hotel_id_list)) + "\n")
    else:
        print(f"Tracking file already exists: {file_path}")

def read_tracking_file(file_path):
    """
    Read the tracking file and return a list of remaining hotel IDs.
    """
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as file:
            return [line.strip() for line in file if line.strip()]
    return []

def write_tracking_file(file_path, remaining_ids):
    """
    Update the tracking file with unprocessed hotel IDs.
    """
    try:
        with open(file_path, "w", encoding="utf-8") as file:
            file.write("\n".join(remaining_ids) + "\n")
    except Exception as e:
        print(f"Error writing to tracking file: {e}")

def append_to_success_file(file_path, hotel_id):
    """
    Append successfully processed hotel ID to the success file.
    """
    try:
        with open(file_path, "a", encoding="utf-8") as file:
            file.write(hotel_id + "\n")
    except Exception as e:
        print(f"Error writing to success file: {e}")

def append_to_not_found_file(file_path, hotel_id):
    """
    Append failed hotel ID to the not found file.
    """
    try:
        with open(file_path, "a", encoding="utf-8") as file:
            file.write(hotel_id + "\n")
    except Exception as e:
        print(f"Error writing to not found file: {e}")

def process_hotels():
    if not os.path.exists(BASE_PATH):
        os.makedirs(BASE_PATH)
        print(f"Created directory: {BASE_PATH}")

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

    for hotel_id in hotel_ids_to_process.copy(): 
        json_path = os.path.join(BASE_PATH, f"{hotel_id}.json")
        
        if os.path.exists(json_path):
            print(f"⏭️⏭️⏭️Skipping ⏭️⏭️⏭️ {hotel_id} - JSON file already exists.")
            append_to_success_file(SUCCESS_FILE, hotel_id)
            hotel_ids_to_process.remove(hotel_id)
            write_tracking_file(TRACKING_FILE, hotel_ids_to_process)
            continue

        try:
            print(f"Processing hotel ID: ✔️{hotel_id}")
            success = save_json(BASE_PATH, hotel_id)
            
            if success:
                append_to_success_file(SUCCESS_FILE, hotel_id)
            else:
                append_to_not_found_file(NOT_FOUND_FILE, hotel_id)

            hotel_ids_to_process.remove(hotel_id)
            
        except Exception as e:
            print(f"❌❌Error processing ❌❌ hotel ID ❌❌❌❌❌{hotel_id}: {e}")
            continue

        write_tracking_file(TRACKING_FILE, hotel_ids_to_process)


    print("Processing completed.")

# Run the function
if __name__ == "__main__":
    process_hotels()


