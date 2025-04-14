import os
import base64
import json
import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class NSNHotelContentMGHoliday:
    def __init__(self):
        # Fetch credentials from environment variables
        self.credentials = {
            "AgencyCode": os.getenv("MGHOLIDAY_AGENCYCODE", "").strip(),
            "Username": os.getenv("MGHOLIDAY_USERNAME", "").strip(),
            "Password": os.getenv("MGHOLIDAY_PASSWORD", "").strip(),
            "base_url": os.getenv("MGHOLIDAY_BASE_URL", "").strip(),
        }

    def hotel_api_authentication(self):
        """
        Prepare API authentication data.
        """
        try:
            agency_code = self.credentials["AgencyCode"]
            username = self.credentials["Username"]
            password = self.credentials["Password"]
            base_url = self.credentials["base_url"]

            # Base64 encode for Basic Authorization (if required)
            authorization_basic = base64.b64encode(f"{username}:{password}".encode()).decode()

            return {
                "AgencyCode": agency_code,
                "Username": username,
                "Password": password,
                "base_url": base_url,
                "Authorization": f"Basic {authorization_basic}",
            }
        except KeyError as e:
            print(f"Missing configuration key: {e}")
            return None
        
    def get_hotel_details(self, hotel_id):
        """
        Fetch hotel details from the API.
        """
        try:
            hotel_id = str(hotel_id).strip()
            credentials = self.hotel_api_authentication()
            if not credentials:
                raise ValueError("Invalid credentials configuration.")

            url = f"{credentials['base_url']}/Hotel/GetHotelDetail"

            # Prepare request payload
            payload = {
                "HotelCode": hotel_id,
                "Login": {
                    "AgencyCode": credentials["AgencyCode"],
                    "Username": credentials["Username"],
                    "Password": credentials["Password"],
                },
            }

            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Accept-Encoding": "application/gzip",
                "Authorization": credentials["Authorization"],  
            }

            response = requests.post(url, headers=headers, data=json.dumps(payload), timeout=30)

            if response.status_code == 200:
                try:
                    return response.json()
                except json.JSONDecodeError:
                    print("Response is not valid JSON.")
                    print(f"Raw response: {response.text}")
                    return None
            else:
                print(f"Failed to fetch hotel details: {response.status_code} - {response.text}")
                return None

        except requests.exceptions.RequestException as e:
            print(f"HTTP Request failed: {e}")
        except ValueError as e:
            print(f"Value error: {e}")
        return None



# Example Usage
if __name__ == "__main__":
    hotel_api = NSNHotelContentMGHoliday()
    hotel_details = hotel_api.get_hotel_details(hotel_id="AD10000040")
    print(hotel_details)
