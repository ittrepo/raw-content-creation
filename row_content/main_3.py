import os
import requests
import json
import os
import requests
import json
import xml.etree.ElementTree as ET
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
    return {"VervotechId": "39654021", "GiataCode": "63919"}

def create_directory(path):
    """
    Create a directory if it does not already exist.
    """
    try:
        os.makedirs(path, exist_ok=True)
        print(f"Directory created: {path}")
    except Exception as e:
        print(f"Error creating directory {path}: {e}")

def create_main_folder(ittid):
    """
    Create the main folder using the provided ittid.
    Example: D:\content_for_hotel_json\row_hotel\itt1111
    """
    main_folder = os.path.join(BASE_DIR, ittid)
    create_directory(main_folder)
    return main_folder

def create_subfolders_for_mapping(main_folder, mapping):
    """
    Create subfolders for VervotechId and GiataCode.
    """
    vervotech_folder = os.path.join(main_folder, f"vervotechid_{mapping['VervotechId']}")
    giata_folder = os.path.join(main_folder, f"giatacode_{mapping['GiataCode']}")
    create_directory(vervotech_folder)
    create_directory(giata_folder)
    return vervotech_folder, giata_folder

def get_provider_hotel_mappings_by_vervotechid(vervotech_id):
    """
    Call the Vervotech Supplier API to fetch provider hotel mappings for the given VervotechId.
    """
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
    """
    Create a folder for each supplier in the base folder (which is the Vervotech folder).
    Each folder name is in the format <providerfamily>_<providerhotelid> in lowercase.
    """
    supplier_folders = {}
    for hotel in provider_hotels:
        provider_id = hotel.get("ProviderHotelId", "").lower()
        provider_family = hotel.get("ProviderFamily", "").lower()
        folder_name = f"{provider_family}_{provider_id}"
        folder_path = os.path.join(base_folder, folder_name)
        create_directory(folder_path)
        # Save folder path using a key based on provider details.
        supplier_folders[(provider_id, provider_family)] = folder_path
    return supplier_folders

def get_provider_content(provider_hotel_id, provider_family):
    """
    Call the Provider Content API to retrieve content for a given supplier.
    """
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
    """
    Save JSON data to a file.
    """
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)
        print(f"JSON data saved to {filepath}")
    except Exception as e:
        print(f"Error saving JSON to {filepath}: {e}")



def get_giata_data(giata_code):
    """Fetch Giata property data in XML format"""
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
    """Parse XML to extract active provider codes"""
    providers = []
    try:
        root = ET.fromstring(xml_data)
        for provider in root.findall('.//propertyCodes/provider'):
            provider_code = provider.get('providerCode').lower()
            for code in provider.findall('code'):
                if code.get('status') != 'inactive':
                    value = code.find('value').text.strip()
                    providers.append((provider_code, value))
    except Exception as e:
        print(f"Error parsing Giata XML: {e}")
    return providers

def create_giata_provider_folders(giata_folder, providers):
    """Create folders for Giata providers"""
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
    
    # Process attributes (if any)
    if element.attrib:
        result["@attributes"] = {}
        for attr, value in element.attrib.items():
            # Remove namespace if present
            if "}" in attr:
                attr = attr.split("}")[1]
            result["@attributes"][attr] = value

    # List of child elements
    children = list(element)
    if children:
        for child in children:
            # Remove namespace from tag name
            tag = child.tag.split("}")[1] if "}" in child.tag else child.tag
            child_dict = xml_to_dict(child)
            if tag in result:
                # If the tag already exists, convert it to a list
                if not isinstance(result[tag], list):
                    result[tag] = [result[tag]]
                result[tag].append(child_dict)
            else:
                result[tag] = child_dict
        
        # If there is text outside of children, add it as well
        if element.text and element.text.strip():
            result["#text"] = element.text.strip()
    else:
        # No children: include text if it exists
        text = element.text.strip() if element.text else ""
        if result:
            if text:
                result["#text"] = text
        else:
            return text

    return result


def fetch_giata_endpoint(url_suffix, giata_code, lang=None):
    """Fetch Giata API data and convert XML response to JSON"""
    # Determine the base URL based on the endpoint
    if url_suffix.startswith("properties/"):
        base_url = "https://multicodes.giatamedia.com/webservice/rest/1.latest/"
    else:
        base_url = "https://ghgml.giatamedia.com/webservice/rest/1.0/"
    
    url = f"{base_url}{url_suffix.format(giata=giata_code, lang=lang)}"
    headers = {
        'Authorization': 'Basic Z2lhdGF8bm9mc2hvbi10b3Vycy5jb206Tm9mc2hvbjEyMy4='
    }
    
    try:
        # Fetch data from the API
        response = requests.post(url, headers=headers)
        response.raise_for_status()
        
        # Parse XML response
        xml_data = response.text
        root = ET.fromstring(xml_data)
        
        # Convert XML to dictionary
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
    # Fetch and parse Giata data
    xml_data = get_giata_data(giata_code)
    if not xml_data:
        return
    
    providers = parse_giata_providers(xml_data)
    if not providers:
        print("No active providers found in Giata data")
        return
    
    # Create provider folders
    provider_folders = create_giata_provider_folders(giata_folder, providers)
    
    # Define endpoints and their file names
    endpoints = [
        ('properties/{giata}', 'basic.json'),
        ('images/{giata}', 'image.json'),
        ('texts/en/{giata}', 'text.json'),
        ('factsheets/{giata}', 'facility.json')
    ]
    
    # Fetch and save data for each provider folder
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


if __name__ == '__main__':
    # Run the process for ittid 'itt1111'
    main("itt1111")
