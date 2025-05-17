import requests
import json
import os
import time
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()

iryx_token = os.getenv("IRYX_TOKEN")
# Directory to save JSON files
output_dir = "D:/content_for_hotel_json/cdn_row_collection/innstanttravel/iryx"
os.makedirs(output_dir, exist_ok=True)

# Step 1: Get Bearer Token
def get_bearer_token():
    url = "https://satgurudmc.com/reseller/oauth2/token"
    payload = 'grant_type=client_credentials&scope=read%3Ahotels-search%20write%3Ahotels-book%20read%3Areservations%20write%3Areservations%20read%3Amapping'
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authorization': f'Basic {iryx_token}'
    }
    response = requests.post(url, headers=headers, data=payload)
    if response.status_code == 200:
        return response.json().get("access_token")
    else:
        raise Exception("Failed to get bearer token")

# Step 2: Fetch hotel type data if 'type' link exists
def fetch_hotel_type(type_url, token):
    headers = {'Authorization': f'Bearer {token}'}
    try:
        response = requests.get(type_url, headers=headers)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        print(f"Error fetching hotel type: {e}")
    return None

# Step 3: Fetch hotel image data
def fetch_hotel_images(hotel_id, token):
    url = f"https://satgurudmc.com/reseller/api/mapping/v1/hotels/{hotel_id}/images?"
    headers = {'Authorization': f'Bearer {token}'}
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Image not found for Hotel ID {hotel_id} | Status: {response.status_code}")
    except Exception as e:
        print(f"Error fetching image for hotel {hotel_id}: {e}")
    return None

# Step 4: Fetch hotel data for a given page
def fetch_hotel_data(page, token):
    url = f"https://satgurudmc.com/reseller/api/mapping/v1/hotels?page={page}"
    headers = {'Authorization': f'Bearer {token}'}
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to fetch data for page {page}: {response.status_code}")
    except Exception as e:
        print(f"Error fetching data for page {page}: {e}")
    return None

# Step 5: Save hotel data to JSON file
def save_hotel_data(hotel, token, type_data=None):
    hotel_id = hotel["id"]
    filename = os.path.join(output_dir, f"{hotel_id}.json")

    # Skip if already saved
    if os.path.exists(filename):
        print(f"File {filename} already exists. Skipping...")
        return

    # Fetch images
    image_data = fetch_hotel_images(hotel_id, token)

    # Combine all data
    data_to_save = {
        "hotel": hotel,
        "type": type_data if type_data else None,
        "images": image_data if image_data else None
    }

    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data_to_save, f, indent=4, ensure_ascii=False)
    print(f"Saved {filename}")


def process_hotel(hotel, token):
    type_data = None
    if "_links" in hotel and "type" in hotel["_links"]:
        type_url = hotel["_links"]["type"]["href"]
        type_data = fetch_hotel_type(type_url, token)
    save_hotel_data(hotel, token, type_data)

def main():
    token = get_bearer_token()
    total_pages = 350553

    for page in range(15791, total_pages + 1):
        print(f"Processing page {page}/{total_pages}")
        data = fetch_hotel_data(page, token)
        if not data:
            continue

        hotels = data.get("_embedded", {}).get("hotels", [])
        if not hotels:
            continue

        # Use ThreadPoolExecutor to process hotels in parallel
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(process_hotel, hotel, token) for hotel in hotels]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Error in thread execution: {e}")

        time.sleep(1)
if __name__ == "__main__":
    main()