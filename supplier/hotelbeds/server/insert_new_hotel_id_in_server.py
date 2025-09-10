import requests
import json

# API endpoint
url = "https://mappingapi.innsightmap.com/hotel/pushhotel"

# Read hotel IDs from file
with open("hotelbeds_new.txt", "r") as file:
    hotel_ids = [line.strip() for line in file if line.strip()]

# Loop through IDs and send requests
for hotel_id in hotel_ids:
    payload = {
        "supplier_code": "hotelbeds",
        "hotel_id": [hotel_id] 
    }

    try:
        response = requests.post(url, headers={"Content-Type": "application/json"}, data=json.dumps(payload))

        print(f"Hotel ID: {hotel_id} → Status: {response.status_code}, Response: {response.text}")

    except requests.exceptions.RequestException as e:
        print(f"❌ Error processing Hotel ID {hotel_id}: {e}")
