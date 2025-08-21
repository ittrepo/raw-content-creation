import os
import json
import requests
import time
from dotenv import load_dotenv
import xmltodict
import urllib.parse

load_dotenv()

BASE_PATH = "D:/content_for_hotel_json/cdn_row_collection/restel_3"

def get_supplier_own_raw_data(hotel_id):
    """
    Fetch raw data from the supplier's API using hotel ID.
    """
    xml_data = f"""<?xml version="1.0" encoding="UTF-8"?>
    <peticion>
    <tipo>15</tipo>
    <nombre>Servicio de información de hotel</nombre>
    <agencia>Agencia prueba</agencia>
    <parametros>
        <codigo>{hotel_id}</codigo>
        <idioma>2</idioma>
    </parametros>
    </peticion>"""

    encoded_xml = urllib.parse.quote(xml_data)
    url = f"http://xml.hotelresb2b.com/xml/listen_xml.jsp?codigousu=ZVYE&clausu=xml514142&afiliacio=RS&secacc=151003&xml={encoded_xml}"

    headers = {
        'Cookie': 'JSESSIONID=aaaodjlEZaLhM_vAad2xz'
    }

    response = requests.get(url, headers=headers, timeout=10)
    if response.status_code == 200:
        try:
            data = xmltodict.parse(response.text)
            return data
        except Exception as e:
            print(f"Error parsing XML for hotel ID {hotel_id}: {e}")
            return None
    else:
        print(f"Failed to fetch data for hotel ID {hotel_id}. Status code: {response.status_code}, Response: {response.text}")
        return None

def save_json(raw_path, hotel_id):
    json_file_path = os.path.join(raw_path, f"{hotel_id}.json")
    data = get_supplier_own_raw_data(hotel_id)
    
    if data is None:
        print(f"Warning: {hotel_id}.json - Data fetch failed. Saving default empty JSON.")
        data = {}
    
    try:
        if not os.path.exists(raw_path):
            os.makedirs(raw_path)
        with open(json_file_path, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=2)
        print(f"{hotel_id}.json saved successfully at {json_file_path}")
        return True
    except Exception as e:
        print(f"Error saving JSON for hotel {hotel_id}: {e}")
        return False

if __name__ == "__main__":
    hotel_id = input("Enter hotel ID: ").strip()
    save_json(BASE_PATH, hotel_id)