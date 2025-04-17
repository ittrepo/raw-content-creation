import xmltodict
import json
import os

# Define the directory paths
xml_directory_path = r'D:/content_for_hotel_json/cdn_row_collection/HotelDetailXML'
json_directory_path = r'D:/content_for_hotel_json/cdn_row_collection/stuba'

# Ensure the JSON directory exists
os.makedirs(json_directory_path, exist_ok=True)

# List all files in the XML directory
files = os.listdir(xml_directory_path)

# Filter out non-XML files
xml_files = [file for file in files if file.endswith('.xml')]

# Process each XML file
for xml_file in xml_files:
    xml_file_path = os.path.join(xml_directory_path, xml_file)
    json_file_path = os.path.join(json_directory_path, xml_file.replace('.xml', '.json'))

    # Read the XML file
    with open(xml_file_path, 'r', encoding='utf-8') as file:
        xml_content = file.read()

    # Parse the XML content
    xml_dict = xmltodict.parse(xml_content)

    # Convert the dictionary to a JSON string
    json_content = json.dumps(xml_dict, indent=4)

    # Write the JSON string to a file
    with open(json_file_path, 'w', encoding='utf-8') as file:
        file.write(json_content)

    print(f"XML file {xml_file} has been converted to JSON and saved as {json_file_path}")
