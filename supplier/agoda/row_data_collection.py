import requests
import xmltodict
import json
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime
import random

load_dotenv()

class HotelContentAgoda:
    def __init__(self):
        self.api_key = os.getenv("AGODA_API_KEY")
        self.base_url = "https://affiliatefeed.agoda.com/datafeeds/feed/getfeed"

    def get_hotel_data(self, hotel_id):
        try:
            url = f"{self.base_url}?apikey={self.api_key}&mhotel_id={hotel_id}&feed_id=19"
            response = requests.get(url)
            
            if response.status_code == 200:
                xml_data = response.content
                data_dict = xmltodict.parse(xml_data)
                json_data = json.dumps(data_dict, indent=4)
                print(json_data)
                return json_data
            else:
                print(f"Failed to fetch data for Hotel ID {hotel_id}: {response.text}")
                return None
        except Exception as e:
            print(f"Error fetching hotel data: {e}")
            return None

    def save_hotel_data(self, hotel_id, save_path):
        json_data = self.get_hotel_data(hotel_id)
        if json_data:
            file_path = os.path.join(save_path, f"{hotel_id}.json")
            with open(file_path, "w", encoding="utf-8") as file:
                file.write(json_data)
            print(f"Hotel data saved: {file_path}")
        else:
            print(f"No data to save for Hotel ID {hotel_id}")

# Example usage
if __name__ == "__main__":
    agoda = HotelContentAgoda()
    hotel_id = "100007"  # Example hotel ID
    save_directory = "D:/AgodaHotelData"  # Specify your save location
    os.makedirs(save_directory, exist_ok=True)
    agoda.save_hotel_data(hotel_id, save_directory)
