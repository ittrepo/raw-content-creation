import requests
import json
from datetime import datetime 
import os
from dotenv import load_dotenv

load_dotenv()

class TravelGateXAPI:
    def __init__(self, api_key, url="https://api.travelgatex.com"):
        self.api_key = api_key
        self.url = url
        self.headers = {
            'Authorization': f'Apikey {self.api_key}',
            'Accept': 'gzip',
            'Connection': 'keep-alive',
            'TGX-Content-Type': 'graphqlx/json',
            'Content-Type': 'application/json'
        }

    def fetch_hotels(self, criteria_hotels, language="en", token=""):
        payload = {
                    "query": (
                        "query ($criteriaHotels: HotelXHotelListInput!, $language: [Language!], $token: String) {"
                        "  hotelX {"
                        "    hotels(criteria: $criteriaHotels, token: $token) {"
                        "      token"
                        "      count"
                        "      edges {"
                        "        node {"
                        "          createdAt"
                        "          updatedAt"
                        "          hotelData {"
                        "            hotelCode"
                        "            hotelName"
                        "            categoryCode"
                        "            chainCode"
                        "            mandatoryFees {"
                        "              duration"
                        "              scope"
                        "              name"
                        "              text"
                        "              price {"
                        "                amount"
                        "                currency"
                        "              }"
                        "            }"
                        "            giataData {"
                        "              id"
                        "            }"
                        "            checkIn {"
                        "              schedule {"
                        "                startTime"
                        "              }"
                        "              minAge"
                        "              instructions(languages: $language) {"
                        "                language"
                        "                text"
                        "              }"
                        "              specialInstructions(languages: $language) {"
                        "                language"
                        "                text"
                        "              }"
                        "            }"
                        "            checkOut {"
                        "              schedule {"
                        "                startTime"
                        "              }"
                        "              minAge"
                        "              instructions(languages: $language) {"
                        "                language"
                        "                text"
                        "              }"
                        "              specialInstructions(languages: $language) {"
                        "                language"
                        "                text"
                        "              }"
                        "            }"
                        "            location {"
                        "              address"
                        "              zipCode"
                        "              city"
                        "              country"
                        "              coordinates {"
                        "                latitude"
                        "                longitude"
                        "              }"
                        "              closestDestination {"
                        "                code"
                        "                available"
                        "                texts(languages: $language) {"
                        "                  text"
                        "                  language"
                        "                }"
                        "                type"
                        "                parent"
                        "              }"
                        "            }"
                        "            contact {"
                        "              email"
                        "              telephone"
                        "              fax"
                        "              web"
                        "            }"
                        "            propertyType {"
                        "              propertyCode"
                        "              name"
                        "            }"
                        "            descriptions(languages: $language) {"
                        "              type"
                        "              texts {"
                        "                language"
                        "                text"
                        "              }"
                        "            }"
                        "            medias {"
                        "              code"
                        "              url"
                        "            }"
                        "            allAmenities {"
                        "              edges {"
                        "                node {"
                        "                  amenityData {"
                        "                    code"
                        "                    amenityCode"
                        "                  }"
                        "                }"
                        "              }"
                        "            }"
                        "          }"
                        "        }"
                        "      }"
                        "    }"
                        "  }"
                        "}"
                    ),
            "variables": {
                "criteriaHotels": criteria_hotels,
                "language": language,
                "token": token
            }
        }
        response = requests.post(self.url, headers=self.headers, json=payload)
        if response.status_code != 200:
            raise Exception(f"API Error: {response.status_code} - {response.text}")
        return response


    def extract_hotel_data(self, response):
        try:
            if response.status_code == 200:
                data = response.json()
                get_token = data.get("data", {}).get("hotelX", {}).get("hotels", {}).get("token", {})

                hotels = data.get("data", {}).get("hotelX", {}).get("hotels", {}).get("edges", [])
                extracted_data = []

                for hotel in hotels:
                    node = hotel.get("node", {})
                    if not node:
                        print(f"Error with hotel data: {hotel}")
                        continue

                    extracted_data.append({"node": node})

                return extracted_data, get_token
            else:
                raise Exception(f"Failed to fetch data: {response.status_code}, {response.text}")

        except Exception as e:
            print(f"An error occurred while extracting hotel data: {str(e)}")
            return [], None

def fetch_and_save_hotels_in_json(criteria_hotels, output_dir):
    api_key = os.getenv("TRAVELGETGTR_API_KEY")
    travelgatex_api = TravelGateXAPI(api_key)

    token = ""
    while token is not None:
        try:
            response = travelgatex_api.fetch_hotels(criteria_hotels, token=token)
            hotel_list, token = travelgatex_api.extract_hotel_data(response)

            # Process and save the hotels
            for hotel_data in hotel_list:
                try:
                    hotel_id = hotel_data.get("node", {}).get("hotelData", {}).get("hotelCode", "unknown")
                    output_file = os.path.join(output_dir, f"{hotel_id}.json")

                    if os.path.exists(output_file):
                        print(f"Skipping........................................................ {hotel_id}, file already exists.")
                        continue

                    # Save hotel data to JSON
                    os.makedirs(output_dir, exist_ok=True)
                    with open(output_file, 'w') as file:
                        json.dump(hotel_data, file, indent=4)
                    print(f"Hotel data saved to {output_file}")
                except Exception as e:
                    print(f"Error saving hotel data: {e}")
                    continue
        except Exception as e:
            print(f"Error fetching hotels: {e}")
            continue

# Example usage
if __name__ == "__main__":
    criteria_hotels = {"access": "32944"}
    output_folder = "D:/content_for_hotel_json/cdn_row_collection/rakuten_TGX_GTRcd"
    fetch_and_save_hotels_in_json(criteria_hotels, output_folder)