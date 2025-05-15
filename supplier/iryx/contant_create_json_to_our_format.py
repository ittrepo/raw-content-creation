import os
import json
from datetime import datetime



def safe_get(d, keys, default=None):
    for key in keys:
        if isinstance(d, dict):
            d = d.get(key)
        else:
            return default
    return d if d is not None else default

def transform_and_save_json(input_path, output_folder):
    with open(input_path, 'r', encoding='utf-8') as file:
        try:
            data = json.load(file)
        except json.JSONDecodeError:
            print(f"❌ Skipping invalid JSON file: {input_path}")
            return

    



    createdAt = datetime.now()
    createdAt_str = createdAt.strftime("%Y-%m-%dT%H:%M:%S")
    created_at_dt = datetime.strptime(createdAt_str, "%Y-%m-%dT%H:%M:%S")
    timeStamp = int(created_at_dt.timestamp())




    hotel = data.get("hotel", {})
    country = hotel.get("country", {})
    city = hotel.get("city", {})
    geolocation = hotel.get("geolocation", {})
    hotel_type = data.get("type", {})
    images_data = data.get("images", {})

    # Handle images
    gallery = images_data.get("_embedded", {}).get("gallery", [])
    images = []
    for item in gallery:
        images.append({
            "picture_id": item.get("id", None),
            "title": item.get("name", None),
            "url": item.get("url", None)
        })

    primary_photo_url = images[0]["url"] if images else None

    address_line_1 = hotel.get("address", None)
    hotel_name = hotel.get("name", None)
    address_query = f"{address_line_1}, {hotel_name}"
    google_map_site_link = f"http://maps.google.com/maps?q={address_query.replace(' ', '+')}" if address_line_1 != None else None

    hotel_code = hotel.get("id", None)

    # Final structured data
    formatted_data = {
        "created": createdAt_str,
        "timestamp": timeStamp,
        "hotel_id": hotel_code,
        "name": hotel.get("name", None),
        "name_local": None,
        "hotel_formerly_name": None,
        "destination_code": city.get("id", None),
        "country_code": country.get("iso", None),
        "brand_text": None,
        "property_type": hotel_type.get("name", None),
        "star_rating": hotel.get("stars", None),
        "chain": None,
        "brand": None,
        "logo": None,
        "primary_photo": primary_photo_url,
        "review_rating": {
            "source": None,
            "number_of_reviews": None,
            "rating_average": None,
            "popularity_score": None
        },
        "policies": {
            "check_in": {
                "begin_time": None,
                "end_time": None,
                "instructions": None,
                "min_age": None
            },
            "checkout": {
                "time": None
            },
            "fees": {
                "optional": None
            },
            "know_before_you_go": None,
            "pets": [],
            "remark": None,
            "child_and_extra_bed_policy": {
                "infant_age": None,
                "children_age_from": None,
                "children_age_to": None,
                "children_stay_free": None,
                "min_guest_age": None
            },
            "nationality_restrictions": None
        },
        "address": {
            "latitude": geolocation.get("latitude", None),
            "longitude": geolocation.get("longitude", None),
            "address_line_1": hotel.get("address", None),
            "address_line_2": None,
            "city": city.get("name", None),
            "state": None,
            "country": country.get("name", None),
            "country_code": country.get("iso", None),
            "postal_code": hotel.get("zipCode", None),
            "full_address": hotel.get("address", None),
            "google_map_site_link": google_map_site_link,
            "local_lang": {
                "latitude": geolocation.get("latitude", None),
                "longitude": geolocation.get("longitude", None),
                "address_line_1": hotel.get("address", None),
                "address_line_2": None,
                "city": city.get("name", None),
                "state": None,
                "country": country.get("name", None),
                "country_code": country.get("iso", None),
                "postal_code": hotel.get("zipCode", None),
                "full_address": hotel.get("address", None),
                "google_map_site_link": google_map_site_link,
            },
            "mapping": {
                "continent_id": None,
                "country_id": country.get("id", None),
                "province_id": None,
                "state_id": None,
                "city_id": city.get("id", None),
                "area_id": None
            }
        },
        "contacts": {
            "phone_numbers": [hotel.get("telephone")] if hotel.get("telephone") else [],
            "fax": [hotel.get("fax")] if hotel.get("fax") else [],
            "email_address": [hotel.get("email")] if hotel.get("email") else [],
            "website": []
        },
        "descriptions": [
            {
                "title": None,
                "text": None
            }
        ],
        "room_type": [
            {
                "room_id": None,
                "title": None,
                "title_lang": None,
                "room_pic": None,
                "description": None,
                "max_allowed": {
                    "total": None,
                    "adults": None,
                    "children": None,
                    "infant": None
                },
                "no_of_room": None,
                "room_size": None,
                "bed_type": [
                    {
                        "description": None,
                        "configuration": [],
                        "max_extrabeds": None
                    }
                ],
                "shared_bathroom": None
            }
        ],
        "spoken_languages": [
            {
                "type": "spoken_languages",
                "title": None,
                "icon": "mdi mdi-translate-variant"
            }
        ],
        "amenities": [
            {
                "type": None,
                "title": None,
                "icon": "mdi mdi-translate-variant"
            }
        ],
        "facilities": [
            {
                "type": None,
                "title": None,
                "icon": "mdi mdi-translate-variant"
            }
        ],
        "hotel_photo": images
    }

    # Save to new JSON file named by hotelCode
    output_path = os.path.join(output_folder, f"{hotel_code}.json")
    with open(output_path, 'w', encoding='utf-8') as outfile:
        json.dump(formatted_data, outfile, indent=4, ensure_ascii=False)

    print(f"✅ Created: {output_path}")



# Main execution
def main():
    source_folder = r"D:\content_for_hotel_json\cdn_row_collection\iryx"
    output_folder = r"D:\content_for_hotel_json\HotelInfo\iryx"
    os.makedirs(output_folder, exist_ok=True)

    for filename in os.listdir(source_folder):
        if filename.endswith(".json"):
            input_path = os.path.join(source_folder, filename)
            transform_and_save_json(input_path, output_folder)

if __name__ == "__main__":
    main()