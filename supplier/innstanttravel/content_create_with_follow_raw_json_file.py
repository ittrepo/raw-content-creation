import json
import os
from datetime import datetime

def transform_hotel_data(input_data):
    createdAt = datetime.now()
    createdAt_str = createdAt.strftime("%Y-%m-%dT%H:%M:%S")
    created_at_dt = datetime.strptime(createdAt_str, "%Y-%m-%dT%H:%M:%S")
    timeStamp = int(created_at_dt.timestamp())



    transformed = {
        "created": createdAt_str,
        "timestamp": timeStamp,
        "hotel_id": str(input_data[0]['id']),
        "name": input_data[0]['name'],
        "name_local": input_data[0]['name'],
        "hotel_formerly_name": input_data[0]['seoname'],
        "destination_code": str(input_data[0]['destinations'][0]['destinationId']),
        "country_code": None,
        "brand_text": None,
        "property_type": None,
        "star_rating": str(input_data[0]['stars']),
        "chain": None,
        "brand": None,
        "logo": None,
        "primary_photo": None,
        "review_rating": {
            "source": None,
            "number_of_reviews": None,
            "rating_average": None,
            "popularity_score": None
        },
        "policies": {
            "check_in": {"begin_time": None, "end_time": None, "instructions": None, "min_age": None},
            "checkout": {"time": None},
            "fees": {"optional": None},
            "know_before_you_go": None,
            "pets": [None],
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
        "address": create_address(input_data[0]),
        "contacts": create_contacts(input_data[0]),
        "descriptions": [{
            "title": None,
            "text": input_data[0]['description']
        }],
        "room_type": [],
        "spoken_languages": [{
            "type": "spoken_languages",
            "title": None,
            "icon": "mdi mdi-translate-variant"
        }],
        "amenities": [{
            "type": None,
            "title": None,
            "icon": "mdi mdi-translate-variant"
        }],
        "facilities": create_facilities(input_data[0]),
        "hotel_photo": create_hotel_photos(input_data[0]),
        "point_of_interests": [{"code": None, "name": None}],
        "nearest_airports": [{"code": None, "name": None}],
        "train_stations": [{"code": None, "name": None}],
        "connected_locations": [{"code": None, "name": None}],
        "stadiums": [{"code": None, "name": None}]
    }

    # Handle primary photo
    main_image_id = input_data[0]['mainImageId']
    for img in input_data[0]['images']:
        if img['id'] == main_image_id:
            transformed['primary_photo'] = create_image_url(img)
            break

    return transformed

def create_address(hotel):
    full_address = hotel['address']
    return {
        "latitude": str(hotel['lat']),
        "longitude": str(hotel['lon']),
        "address_line_1": hotel['address'],
        "address_line_2": None,
        "city": None,
        "state": None,
        "country": None,
        "country_code": None,
        "postal_code": hotel['zip'],
        "full_address": hotel['address'],
        "google_map_site_link": f"http://maps.google.com/maps?q={full_address.replace(' ', '+')}",
        "local_lang": {
            "latitude": str(hotel['lat']),
            "longitude": str(hotel['lon']),
            "address_line_1": hotel['address'],
            "address_line_2": None,
            "city": None,
            "state": None,
            "country": None,
            "country_code": None,
            "postal_code": hotel['zip'],
            "full_address": hotel['address'],
            "google_map_site_link": None
        },
        "mapping": {
            "continent_id": None,
            "country_id": None,
            "province_id": None,
            "state_id": None,
            "city_id": None,
            "area_id": None
        }
    }

def create_contacts(hotel):
    return {
        "phone_numbers": [hotel['phone']],
        "fax": [hotel['fax']],
        "email_address": None,
        "website": None
    }

def create_facilities(hotel):
    facilities = []
    seen = set()
    
    # Combine tags and list while preserving order
    for item in hotel['facilities']['tags'] + hotel['facilities']['list']:
        if item.strip() and item not in seen:
            seen.add(item)
            facilities.append({
                "type": item,
                "title": item,
                "icon": "mdi mdi-translate-variant"
            })
    return facilities

def create_hotel_photos(hotel):
    photos = []
    for img in hotel['images']:
        photos.append({
            "picture_id": str(img['id']),
            "title": img['title'],
            "url": create_image_url(img)
        })
    return photos

def create_image_url(img):
    base_url = "https://cdn-images.innstant-servers.com"
    dimensions = f"{img['width']}x{img['height']}" if img['width'] and img['height'] else "0x0"
    path = os.path.dirname(img['url'])
    filename = f"{img['id']}.jpg"
    return f"{base_url}/{dimensions}/{path}/{filename}"

# Read input file
with open('1.json', 'r', encoding='utf-8') as f:
    input_data = json.load(f)

# Transform data
transformed_data = transform_hotel_data(input_data)

# Write output to same file
with open('1.json', 'w', encoding='utf-8') as f:
    json.dump(transformed_data, f, indent=2, ensure_ascii=False)

print("File transformed and saved successfully!")