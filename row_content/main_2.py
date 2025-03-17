import os
import requests
import json
import xml.etree.ElementTree as ET

# Base directory where all folders will be created.
BASE_DIR = r"D:\content_for_hotel_json\row_hotel"

def get_hotel_mapping(ittid):
    """
    Simulate a database query that returns the mapping for a given ittid.
    For example, for 'itt1111' it returns:
      VervotechId: 39654021
      GiataCode: 63919
    """
    # Replace with real database call as needed.
    return {"VervotechId": "39758740", "GiataCode": "602756"}

def create_directory(path):
    try:
        os.makedirs(path, exist_ok=True)
        print(f"Directory created: {path}")
    except Exception as e:
        print(f"Error creating directory {path}: {e}")

def create_main_folder(ittid):
    main_folder = os.path.join(BASE_DIR, ittid)
    create_directory(main_folder)
    return main_folder

def create_subfolders_for_mapping(main_folder, mapping):
    vervotech_folder = os.path.join(main_folder, f"vervotechid_{mapping['VervotechId']}")
    giata_folder = os.path.join(main_folder, f"giatacode_{mapping['GiataCode']}")
    create_directory(vervotech_folder)
    create_directory(giata_folder)
    return vervotech_folder, giata_folder

def get_provider_hotel_mappings_by_vervotechid(vervotech_id):
    url = f"https://hotelmapping.vervotech.com/api/3.0/mappings/GetProviderHotelMappingsByVervotechId?vervotechId={vervotech_id}"
    headers = {
        'accountid': 'gtrs',
        'apikey': 'b0ae90d7-2507-4751-ba4d-d119827c1ed2'
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        print("Received provider hotel mappings:", data)
        return data
    except Exception as e:
        print("Error fetching provider hotel mappings:", e)
        return None

def create_supplier_folders(base_folder, provider_hotels):
    supplier_folders = {}
    for hotel in provider_hotels:
        provider_id = hotel.get("ProviderHotelId", "").lower()
        provider_family = hotel.get("ProviderFamily", "").lower()
        folder_name = f"{provider_family}_{provider_id}"
        folder_path = os.path.join(base_folder, folder_name)
        create_directory(folder_path)
        supplier_folders[(provider_id, provider_family)] = folder_path
    return supplier_folders

def get_provider_content(provider_hotel_id, provider_family):
    url = "https://hotelmapping.vervotech.com/api/3.0/content/GetProviderContentByProviderHotelIds"
    payload = json.dumps({
        "ProviderHotelIdentifiers": [
            {
                "ProviderHotelId": provider_hotel_id,
                "ProviderFamily": provider_family
            }
        ]
    })
    headers = {
        'accountid': 'gtrs',
        'apikey': 'b0ae90d7-2507-4751-ba4d-d119827c1ed2',
        'Content-Type': 'application/json'
    }
    try:
        response = requests.post(url, headers=headers, data=payload)
        response.raise_for_status()
        data = response.json()
        print(f"Received content for {provider_family} {provider_hotel_id}:", data)
        return data
    except Exception as e:
        print(f"Error fetching provider content for {provider_family} {provider_hotel_id}:", e)
        return None

def save_json_to_file(data, filepath):
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)
        print(f"JSON data saved to {filepath}")
    except Exception as e:
        print(f"Error saving JSON to {filepath}: {e}")

def get_giata_data(giata_code):
    url = f"https://multicodes.giatamedia.com/webservice/rest/1.latest/properties/{giata_code}"
    headers = {
        'Authorization': 'Basic Z2lhdGF8bm9mc2hvbi10b3Vycy5jb206Tm9mc2hvbjEyMy4='
    }
    try:
        response = requests.post(url, headers=headers)
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"Error fetching Giata data: {e}")
        return None

def parse_giata_providers(xml_data):
    providers = []
    try:
        root = ET.fromstring(xml_data)
        for provider in root.findall('.//propertyCodes/provider'):
            provider_code = provider.get('providerCode').lower()
            # Handle InnstantTravel specifically
            if provider_code == "innstanttravel":
                for code in provider.findall('code'):
                    if code.get('status') != 'inactive':
                        value = code.find('value').text.strip()
                        providers.append(("innstanttravel", value))  # Force provider_code to match
            else:
                for code in provider.findall('code'):
                    if code.get('status') != 'inactive':
                        value = code.find('value').text.strip()
                        providers.append((provider_code, value))
    except Exception as e:
        print(f"Error parsing Giata XML: {e}")
    return providers

def create_giata_provider_folders(giata_folder, providers):
    folders = {}
    for provider_code, code_value in providers:
        folder_name = f"{provider_code}_{code_value}".lower().replace(" ", "_")
        folder_path = os.path.join(giata_folder, folder_name)
        create_directory(folder_path)
        folders[(provider_code, code_value)] = folder_path
    return folders

def xml_to_dict(element):
    """Recursively convert XML element to dictionary, including namespaces, attributes, and text"""
    result = {}
    
    if element.attrib:
        result["@attributes"] = {}
        for attr, value in element.attrib.items():
            if "}" in attr:
                attr = attr.split("}")[1]
            result["@attributes"][attr] = value

    children = list(element)
    if children:
        for child in children:
            tag = child.tag.split("}")[1] if "}" in child.tag else child.tag
            child_dict = xml_to_dict(child)
            if tag in result:
                if not isinstance(result[tag], list):
                    result[tag] = [result[tag]]
                result[tag].append(child_dict)
            else:
                result[tag] = child_dict
        
        if element.text and element.text.strip():
            result["#text"] = element.text.strip()
    else:
        text = element.text.strip() if element.text else ""
        if result:
            if text:
                result["#text"] = text
        else:
            return text

    return result

def fetch_giata_endpoint(url_suffix, giata_code, lang=None):
    """Fetch Giata API data and convert XML response to JSON"""
    if url_suffix.startswith("properties/"):
        base_url = "https://multicodes.giatamedia.com/webservice/rest/1.latest/"
    else:
        base_url = "https://ghgml.giatamedia.com/webservice/rest/1.0/"
    
    url = f"{base_url}{url_suffix.format(giata=giata_code, lang=lang)}"
    headers = {
        'Authorization': 'Basic Z2lhdGF8bm9mc2hvbi10b3Vycy5jb206Tm9mc2hvbjEyMy4='
    }
    
    try:
        response = requests.post(url, headers=headers)
        response.raise_for_status()
        
        xml_data = response.text
        root = ET.fromstring(xml_data)
        
        json_data = xml_to_dict(root)
        return json_data
    
    except Exception as e:
        print(f"Error fetching or processing {url_suffix}: {e}")
        return None

def save_json_file(data, filepath):
    """Save JSON data to a file"""
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        print(f"Saved JSON: {filepath}")
    except Exception as e:
        print(f"Error saving JSON file {filepath}: {e}")

def process_giata_data(giata_folder, giata_code):
    """Main processing function for Giata data"""
    xml_data = get_giata_data(giata_code)
    if not xml_data:
        return
    
    providers = parse_giata_providers(xml_data)
    if not providers:
        print("No active providers found in Giata data")
        return
    
    provider_folders = create_giata_provider_folders(giata_folder, providers)
    
    endpoints = [
        ('properties/{giata}', 'basic.json'),
        ('images/{giata}', 'image.json'),
        ('texts/en/{giata}', 'text.json'),
        ('factsheets/{giata}', 'facility.json')
    ]
    
    for folder_path in provider_folders.values():
        for url_suffix, filename in endpoints:
            json_data = fetch_giata_endpoint(
                url_suffix, 
                giata_code=giata_code,
                lang='en' if 'texts' in url_suffix else None
            )
            if json_data:
                file_path = os.path.join(folder_path, filename)
                save_json_file(json_data, file_path)

# -------------------------------
# New functions for Innstant travel API
# -------------------------------

def fetch_innstanttravel_data(hotel_id):
    url = f"https://static-data.innstant-servers.com/hotels/{hotel_id}"
    headers = {
        'aether-application-key': '$2y$10$Z3AZlsF.gtIUUwa4LE2Bqugx9hrJp76ksNpc44oM2.TOINkBtjj1m',
        'aether-access-token': '$2y$10$R2/rYvapSB4R2sC4BlIOyeA8EuUjBaL1Gm5id372wWeh0m6e1QwLO'
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()  
        print(f"Received Innstant travel data for hotel {hotel_id}:", data)
        return data
    except Exception as e:
        print(f"Error fetching Innstant travel data for hotel {hotel_id}: {e}")
        return None

def process_innstanttravel_data(giata_folder, vervotech_folder, innstant_ids):
    for hotel_id in innstant_ids:
        folder_name = f"innstanttravel_{hotel_id}"
        # Build potential folder paths under each subfolder
        path_giata = os.path.join(giata_folder, folder_name)
        path_vervotech = os.path.join(vervotech_folder, folder_name)
        
        # Check if the folder exists in the giata folder first, then in the vervotech folder
        if os.path.exists(path_giata):
            folder_path = path_giata
            print(f"Found existing folder in giata: {folder_path}")
        elif os.path.exists(path_vervotech):
            folder_path = path_vervotech
            print(f"Found existing folder in vervotech: {folder_path}")
        else:
            # Create the folder in the giata folder by default
            folder_path = path_giata
            create_directory(folder_path)
            print(f"Created new innstanttravel folder: {folder_path}")
        
        # Fetch and save data
        json_data = fetch_innstanttravel_data(hotel_id)
        if json_data:
            file_name = f"own_innstanttravel_{hotel_id}.json"
            file_path = os.path.join(folder_path, file_name)
            save_json_file(json_data, file_path)


def main(ittid):
    # Step 1: Retrieve hotel mapping data (simulate database lookup)
    mapping = get_hotel_mapping(ittid)
    
    # Step 2: Create the main folder for the ittid.
    main_folder = create_main_folder(ittid)
    
    # Step 3: Create subfolders for VervotechId and GiataCode.
    vervotech_folder, giata_folder = create_subfolders_for_mapping(main_folder, mapping)
    
    # Step 4: Call the API to get provider hotel mappings using the VervotechId.
    provider_mappings = get_provider_hotel_mappings_by_vervotechid(mapping["VervotechId"])
    if provider_mappings and "ProviderHotels" in provider_mappings:
        # Step 5: Create supplier folders in the Vervotech folder.
        supplier_folders = create_supplier_folders(vervotech_folder, provider_mappings["ProviderHotels"])
        
        # Step 6: For each supplier, get provider content and save the JSON response.
        for hotel in provider_mappings["ProviderHotels"]:
            provider_id = hotel.get("ProviderHotelId", "")
            provider_family = hotel.get("ProviderFamily", "")
            content = get_provider_content(provider_id, provider_family)
            if content is not None:
                key = (provider_id.lower(), provider_family.lower())
                folder_path = supplier_folders.get(key)
                if folder_path:
                    file_path = os.path.join(folder_path, "response.json")
                    save_json_to_file(content, file_path)
    else:
        print("No provider hotel mappings found.")
    
    # Step 7: Process Giata data
    process_giata_data(giata_folder, mapping['GiataCode'])
    
    # Step 8: Process Innstant travel data for all IDs
    xml_data = get_giata_data(mapping['GiataCode'])
    if xml_data:
        providers = parse_giata_providers(xml_data)
        # Extract all InnstantTravel IDs
        innstant_ids = [value for (code, value) in providers if code == "innstanttravel"]
        if innstant_ids:
            process_innstanttravel_data(giata_folder, vervotech_folder, innstant_ids)

if __name__ == '__main__':
    # Run the process for ittid 'itt1112'
    main("itt1112")
