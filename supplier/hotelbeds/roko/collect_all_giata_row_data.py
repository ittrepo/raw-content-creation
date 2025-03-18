import requests
import xmltodict
import json
from dotenv import load_dotenv
import os

load_dotenv()


def giata_to_get_basic_info(supplier_name, hotel_id):
    GIATA_API_KEY = os.getenv("GIATA_API_KEY")

    # print("hotel_id", hotel_id)
    url = f"https://multicodes.giatamedia.com/webservice/rest/1.latest/properties/gds/{supplier_name}/{hotel_id}"
    headers = {
        'Authorization': f'Basic {GIATA_API_KEY}'
    }
    
    response = requests.post(url, headers=headers)

    
    if response.status_code == 200:
        xml_data = response.text
        
        dict_data = xmltodict.parse(xml_data)

        giata_id = dict_data["properties"]["property"]["@giataId"]
        
        json_data = json.dumps(dict_data, indent=4)
        return json_data, giata_id
    else:
        print(f"Failed to fetch data. Status Code: {response.status_code}")
        return None


def giata_to_get_image_info(giata_id):
    GIATA_API_KEY = os.getenv("GIATA_API_KEY")
    
    url = f"https://ghgml.giatamedia.com/webservice/rest/1.0/images/{giata_id}"
    headers = {
        'Authorization': f'Basic {GIATA_API_KEY}'
    }
    
    response = requests.post(url, headers=headers)
    
    if response.status_code == 200:
        xml_data = response.text
        
        dict_data = xmltodict.parse(xml_data)
        
        json_data = json.dumps(dict_data, indent=4)
        return json_data
    else:
        print(f"Failed to fetch data. Status Code: {response.status_code}")
        return None
    


def giata_to_get_facility_info(giata_id):
    GIATA_API_KEY = os.getenv("GIATA_API_KEY")
    
    url = f"https://ghgml.giatamedia.com/webservice/rest/1.0/factsheets/{giata_id}"
    headers = {
        'Authorization': f'Basic {GIATA_API_KEY}'
    }
    
    response = requests.post(url, headers=headers)
    
    if response.status_code == 200:
        xml_data = response.text
        
        dict_data = xmltodict.parse(xml_data)
        
        json_data = json.dumps(dict_data, indent=4)
        return json_data
    else:
        print(f"Failed to fetch data. Status Code: {response.status_code}")
        return None
    


def giata_to_get_text_info(giata_id):
    GIATA_API_KEY = os.getenv("GIATA_API_KEY")
    
    url = f"https://ghgml.giatamedia.com/webservice/rest/1.0/texts/en/{giata_id}"
    headers = {
        'Authorization': f'Basic {GIATA_API_KEY}'
    }
    
    response = requests.post(url, headers=headers)
    
    if response.status_code == 200:
        xml_data = response.text
        
        dict_data = xmltodict.parse(xml_data)
        
        json_data = json.dumps(dict_data, indent=4)
        return json_data
    else:
        print(f"Failed to fetch data. Status Code: {response.status_code}")
        return None




def save_json(giata_path, hotel_id, supplier):
    base_path = os.path.join(giata_path, hotel_id)

    if not os.path.exists(base_path):
        os.makedirs(base_path)

    giata_dir = os.path.join(base_path, "giata")

    if not os.path.exists(giata_dir):
        os.makedirs(giata_dir)

    def fetch_and_save_json(fetch_function, file_name, *args):
        json_data = fetch_function(*args)

        if json_data is None or "Failed to fetch data. Status Code: 404" in str(json_data):
            print(f"Warning: {file_name} - Data fetch failed. Saving default empty JSON.")
            json_data = "{}" 
        
        # Save JSON data
        full_file_path = os.path.join(giata_dir, file_name)
        with open(full_file_path, "w", encoding="utf-8") as file:
            file.write(json_data)

        print(f"{file_name} saved successfully at {full_file_path}")

    # Fetch and save JSON data
    json_data_for_basic, giata_id = giata_to_get_basic_info(supplier, hotel_id)

    # Save Basic Info JSON
    fetch_and_save_json(lambda *args: json_data_for_basic, f"basic_{hotel_id}.json")

    # Save Image Info JSON
    fetch_and_save_json(giata_to_get_image_info, f"image_{hotel_id}.json", giata_id)

    # Save Facility Info JSON
    fetch_and_save_json(giata_to_get_facility_info, f"facility_{hotel_id}.json", giata_id)

    # Save Text Info JSON
    
    fetch_and_save_json(giata_to_get_text_info, f"text_{hotel_id}.json", giata_id)


HOTEL_ID_LIST = "D:/Rokon/ofc_git/row_content_create/hotel_id_count_function/hotelbeds_hotel_id_list.txt"
TRACKING_FILE = "D:/Rokon/ofc_git/row_content_create/hotel_id_count_function/hotelbeds_tracking_file.txt"
NOT_FOUND_FILE = "D:/Rokon/ofc_git/row_content_create/hotel_id_count_function/hotelbeds_not_found.txt"

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



    supplier = "hotelbeds"
    base_path = f"D:/content_for_hotel_json/raw_hotel_info/{supplier}"

    for hotel_id in hotel_ids_to_process:
        try:
            print(f"Processing hotel ID: {hotel_id}")
            save_json(base_path, hotel_id, supplier)
        except Exception as e:
            print(f"Error processing hotel ID {hotel_id}: {e}")
            append_to_not_found_file(NOT_FOUND_FILE, hotel_id)
            continue 

        hotel_ids_to_process.remove(hotel_id)
        write_tracking_file(TRACKING_FILE, hotel_ids_to_process)

    print("Processing completed.")

# Run the function
process_hotels()


