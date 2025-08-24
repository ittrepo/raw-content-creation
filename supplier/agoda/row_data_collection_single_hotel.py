import os
import json
import requests
import time
from dotenv import load_dotenv
import xml.etree.ElementTree as ET
import xmltodict

# Load environment variables
load_dotenv()

# Constants
BASE_PATH = "D:/content_for_hotel_json/cdn_row_collection/agoda_1"


REQUEST_DELAY = 1



def get_supplier_own_raw_data(hotel_id):
    """
    Fetch hotel data from the TBO API and convert XML to JSON-serializable dict.
    """
    url = f"http://affiliatefeed.agoda.com/datafeeds/feed/getfeed?feed_id=19&apikey=3ce93b4c-3bb4-4f0f-91c9-6e861c2bf7fb&site_id=1793599&mhotel_id={hotel_id}"

    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            # Convert XML to Python dict
            data_dict = xmltodict.parse(response.content)
            return data_dict
        else:
            print(f"Failed to fetch data for hotel {hotel_id}. Status code: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error fetching data for hotel {hotel_id}: {e}")
        return None
    

def save_json(raw_path, hotel_id):
    json_file_path = os.path.join(raw_path, f"{hotel_id}.json")
    data = get_supplier_own_raw_data(hotel_id)
    
    if data is None:
        print(f"Warning: {hotel_id}.json - Data fetch failed. Saving default empty JSON.")
        data = {}
    
    try:
        with open(json_file_path, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=2)
        print(f"{hotel_id}.json saved successfully at {json_file_path}")
        return True
    except Exception as e:
        print(f"Error saving JSON for hotel {hotel_id}: {e}")
        return False



def process_hotels():
    """
    Process hotels by fetching data and saving it as JSON files.
    """
    hotel_ids = ["1622759"]  

    raw_path = BASE_PATH
    os.makedirs(raw_path, exist_ok=True)

    for hotel_id in hotel_ids:
        print(f"Processing hotel ID: {hotel_id}")
        if save_json(raw_path, hotel_id):
            print(f"Successfully processed hotel ID: {hotel_id}")
        else:
            print(f"Failed to process hotel ID: {hotel_id}")
        time.sleep(REQUEST_DELAY)  
# Run the function

if __name__ == "__main__":
    process_hotels()