import requests
import xmltodict
import json
from dotenv import load_dotenv
import os

load_dotenv()

def giata_to_get_basic_info(supplier_name, hotel_id):
    GIATA_API_KEY = os.getenv("GIATA_API_KEY")
    

    url = f"https://multicodes.giatamedia.com/webservice/rest/1.latest/properties/gds/{supplier_name}/{hotel_id}"
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

def save_json(base_path, hotel_id, supplier):
    json_data = giata_to_get_basic_info(supplier, hotel_id)
    if json_data is None:
        print("No JSON data to save.")
        return

    dir_path = os.path.join(base_path, hotel_id)
    
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    
    file_name = f"basic_{hotel_id}.json"
    full_file_path = os.path.join(dir_path, file_name)
    
    with open(full_file_path, "w", encoding="utf-8") as file:
        file.write(json_data)
    
    print(f"JSON data saved successfully at {full_file_path}")


base_path = "D:/content_for_hotel_json/raw_hotel_info/ratehawk"
hotel_id = "country_del_sol_complejo_turistico"
supplier = "ratehawk2"

save_json(base_path, hotel_id, supplier)
