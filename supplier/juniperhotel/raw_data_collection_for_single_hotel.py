import os
import time
import requests
from dotenv import load_dotenv
import json
import xmltodict

load_dotenv()

BASE_PATH = "D:/content_for_hotel_json/cdn_row_collection/juniper_new"

def get_supplier_own_raw_data(hotel_id):
    JUNIPER_USER = os.getenv("JUNIPER_USER")
    JUNIPER_PASSWORD = os.getenv("JUNIPER_PASSWORD")

    url = "http://juniper-xmlcredit.roibos.com/webservice/jp/operations/staticdatatransactions.asmx"

    payload = f"""
    <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns="http://www.juniper.es/webservice/2007/">
        <soapenv:Header/>
        <soapenv:Body>
            <HotelContent>
                <HotelContentRQ Version="1" Language="en">
                    <Login Password="{JUNIPER_PASSWORD}" Email="{JUNIPER_USER}"/>
                    <HotelContentList>
                        <Hotel Code="{hotel_id}"/>
                    </HotelContentList>
                </HotelContentRQ>
            </HotelContent>
        </soapenv:Body>
    </soapenv:Envelope>
    """

    headers = {
        'Content-Type': 'text/xml;charset=UTF-8',
        'SOAPAction': '"http://www.juniper.es/webservice/2007/HotelContent"'
    }

    try:
        response = requests.post(url, headers=headers, data=payload, timeout=20)

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
    except requests.exceptions.RequestException as e:
        print(f"Request error for hotel ID {hotel_id}: {e}")
        return None

def save_json(raw_path, hotel_id, data):
    json_file_path = os.path.join(raw_path, f"{hotel_id}.json")

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
    data = get_supplier_own_raw_data(hotel_id)
    save_json(BASE_PATH, hotel_id, data)