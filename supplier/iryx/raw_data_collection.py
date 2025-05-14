import requests
import json
import os
import time
from dotenv import load_dotenv

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
        response = requests.get(type_url, headers=headers, data="")
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        print(f"Error fetching hotel type: {e}")
    return None

# Step 3: Fetch hotel data for a given page
def fetch_hotel_data(page, token):
    url = f"https://satgurudmc.com/reseller/api/mapping/v1/hotels?page={page}"
    headers = {'Authorization': f'Bearer {token}'}
    try:
        response = requests.get(url, headers=headers, data="")
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to fetch data for page {page}: {response.status_code}")
    except Exception as e:
        print(f"Error fetching data for page {page}: {e}")
    return None

# Step 4: Save hotel data to JSON file
def save_hotel_data(hotel, type_data=None, image_data=None):
    hotel_id = hotel["id"]
    filename = os.path.join(output_dir, f"{hotel_id}.json")

    # Check if the file already exists
    if os.path.exists(filename):
        print(f"File {filename} already exists. Skipping...")
        return

    # Combine hotel data with type and image data if they exist
    data_to_save = {"hotel": hotel}
    if type_data:
        data_to_save["type"] = type_data
    if image_data:
        data_to_save["image"] = image_data

    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data_to_save, f, indent=4, ensure_ascii=False)
    print(f"Saved {filename}")

# Main function to process all pages
def main():
    # Get bearer token
    token = get_bearer_token()

    # Iterate through all pages (1 to 350553)
    total_pages = 350553
    for page in range(3774, total_pages + 1):
        print(f"Processing page {page}/{total_pages}")

        # Fetch hotel data for the current page
        data = fetch_hotel_data(page, token)
        if not data:
            continue

        hotels = data.get("_embedded", {}).get("hotels", [])

        # Process each hotel
        for hotel in hotels:
            type_data = None
            image_data = None

            # Check if 'type' link exists in _links
            if "_links" in hotel:
                if "type" in hotel["_links"]:
                    type_url = hotel["_links"]["type"]["href"]
                    type_data = fetch_hotel_type(type_url, token)

                if "self" in hotel["_links"]:
                    image_url = hotel["_links"]["self"]["href"]
                    # Assuming fetch_hotel_data can also fetch image data if needed
                    # image_data = fetch_hotel_data(image_url, token)

            # Save hotel data to JSON file
            save_hotel_data(hotel, type_data, image_data)

        # Add a delay to avoid hitting rate limits
        time.sleep(1)

if __name__ == "__main__":
    main()
