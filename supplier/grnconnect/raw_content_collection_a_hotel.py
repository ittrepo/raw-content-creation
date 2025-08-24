import os
import requests
import json

# adjust as needed
BASE_PATH = "D:/content_for_hotel_json/cdn_row_collection/grnconnect_new"
API_KEY = "cda7a3d1a85a048030eca511a2805c59"
HEADERS = {
    'Content-Type': 'application/json',
    'Accept': 'application/json',
    'Accept-Encoding': 'application/gzip',
    'api-key': API_KEY
}
CITY_HEADERS = {
    'Content-Type': 'application/json',
    'Accept': 'application/json',
    'API-key': API_KEY
}

def get_supplier_own_raw_data(hotel_id):
    """
    Fetch hotel, country, city and image info for the given hotel_id,
    save it as JSON to BASE_PATH/hotel_id.json (including hotel_code),
    and return the combined dict.
    """
    # ensure output directory exists
    os.makedirs(BASE_PATH, exist_ok=True)

    # 1) hotel info
    hotel_url = f"https://api-sandbox.grnconnect.com/api/v3/hotels?hcode={hotel_id}&version=2.0"
    resp = requests.get(hotel_url, headers=HEADERS)
    resp.raise_for_status()
    hotel_payload = resp.json()
    hotel = hotel_payload['hotels'][0]

    # 2) country info
    country_code = hotel.get('country')
    country_url = f"https://api-sandbox.grnconnect.com/api/v3/countries/{country_code}"
    resp = requests.get(country_url, headers=HEADERS)
    resp.raise_for_status()
    country = resp.json().get('country', {})

    # 3) city info
    city_code = hotel.get('city_code')
    city_url = f"https://api-sandbox.grnconnect.com/api/v3/cities/{city_code}?version=2.0"
    resp = requests.get(city_url, headers=CITY_HEADERS)
    resp.raise_for_status()
    city = resp.json().get('city', {})

    # 4) images
    images_url = (
        f"https://api-sandbox.grnconnect.com/api/v3/hotels/"
        f"{hotel_id}/images?version=2.0"
    )
    resp = requests.get(images_url, headers=HEADERS)
    resp.raise_for_status()
    images = resp.json().get('images', {}).get('regular', [])

    # combine into single dict, including hotel_code
    data = {
        'hotel_code': hotel_id,
        'hotel': hotel,
        'country': country,
        'city': city,
        'images': images
    }

    # write out to file
    out_path = os.path.join(BASE_PATH, f"{hotel_id}.json")
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    return data

# Example usage:
if __name__ == "__main__":
    hotel_id = "1000059"
    combined = get_supplier_own_raw_data(hotel_id)
    # print(json.dumps(combined, indent=2))
