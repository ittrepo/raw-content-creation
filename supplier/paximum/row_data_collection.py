import requests
import json
from dotenv import load_dotenv
import os
from io import StringIO 
import pandas as pd



load_dotenv()

def authentication_paximum():
    paximum_token = os.getenv("PAXIMUM_TOKEN")
    paximum_agency = os.getenv("PAXIMUM_AGENCY")
    paximum_user = os.getenv("PAXIMUM_USER")
    paximum_password = os.getenv("PAXIMUM_PASSWORD")
    
    url = "http://service.stage.paximum.com/v2/api/authenticationservice/login"

    payload = json.dumps({
        "Agency": paximum_agency,
        "User": paximum_user,
        "Password": paximum_password
    })
    
    headers = {
        'Content-Type': 'application/json',
        'Authorization': paximum_token
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    if response.status_code == 200:
        try:
            df = pd.read_json(StringIO(response.text))
            token = df.get("body").get("token")
            return token
        except Exception as e:
            print("Error parsing token:", e)
            return None
    else:
        print(f"Failed to authenticate. Status code: {response.status_code}, Response: {response.text}")
        return None

def get_supplier_own_raw_data(hotel_id):
    token = authentication_paximum()
    # print(token)
    if not token:
        print(f"Authentication failed for hotel ID {hotel_id}.")
        return None

    url = "http://service.stage.paximum.com/v2/api/productservice/getproductInfo"

    payload = {
        "productType": 2,
        "ownerProvider": 2,
        "product": hotel_id,
        "culture": "en-US"
    }
    
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }

    response = requests.post(url, headers=headers, json=payload)  
    if response.status_code == 200:
        try:
            return response.json()  
        except Exception as e:
            print(f"Error parsing JSON for hotel ID {hotel_id}: {e}")
            return None
    else:
        print(f"Failed to fetch data for hotel ID {hotel_id}. Status code: {response.status_code}")
        return None


def save_json(raw_path, hotel_id):
    """Fetch data and save it as an actual JSON file in the specified path."""
    json_file_path = os.path.join(raw_path, f"{hotel_id}.json")

    json_data = get_supplier_own_raw_data(hotel_id)

    if json_data is None:
        print(f"Warning: {hotel_id}.json - Data fetch failed. Saving default empty JSON.")
        json_data = {}

    # Save JSON correctly
    with open(json_file_path, "w", encoding="utf-8") as file:
        json.dump(json_data, file, indent=4)

    print(f"{hotel_id}.json saved successfully at {json_file_path}")



HOTEL_ID_LIST = "D:/Rokon/ofc_git/row_content_create/hotel_id_count_function/paximum/paximumhotel_hotel_id_list.txt"
TRACKING_FILE = "D:/Rokon/ofc_git/row_content_create/hotel_id_count_function/paximum/paximumhotel_tracking_file.txt"
NOT_FOUND_FILE = "D:/Rokon/ofc_git/row_content_create/hotel_id_count_function/paximum/paximumhotel_not_found.txt"

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



    supplier = "paximum"
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



