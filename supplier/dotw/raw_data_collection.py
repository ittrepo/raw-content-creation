import requests
import xml.etree.ElementTree as ET
import json
import os


def xml_element_to_dict(element):
    """Convert XML element to nested dictionary with attributes"""
    result = element.attrib.copy()
    if element.text and element.text.strip():
        result['_text'] = element.text.strip()

    for child in element:
        child_data = xml_element_to_dict(child)
        if child.tag in result:
            if not isinstance(result[child.tag], list):
                result[child.tag] = [result[child.tag]]
            result[child.tag].append(child_data)
        else:
            result[child.tag] = child_data

    return result


def process_city(city_code, output_dir):
    """Process a single city code and return True if successful"""
    url = "https://www.xmldev.dotwconnect.com/gatewayV4.dotw"
    headers = {
        'Content-Type': 'application/xml',
        'Cookie': 'PHPSESSID=3e62394000d0589af852105f9d7cb26e'
    }
    
    payload = f"""<customer>
                    <username>kam786</username>
                    <password>98aa96f33fd167e34910a1ee3727d2e9</password>
                    <id>2050945</id>
                    <source>1</source>
                    <product>hotel</product>
                    <language>en</language>
                    <request command="searchhotels">
                        <bookingDetails>
                            <fromDate>2018-01-01</fromDate>
                            <toDate>2025-03-05</toDate>
                            <currency>416</currency>
                            <rooms no="5">
                                <room runno="0">
                                    <adultsCode>1</adultsCode>
                                    <children no="0"></children>
                                    <rateBasis>-1</rateBasis>
                                </room>
                            </rooms>
                        </bookingDetails>
                        <return>
                            <getRooms>true</getRooms>
                            <filters xmlns:a="http://us.dotwconnect.com/xsd/atomicCondition" xmlns:c="http://us.dotwconnect.com/xsd/complexCondition">
                                <city>{city_code}</city>
                                <noPrice>true</noPrice>
                            </filters>
                            <fields>
                                <field>preferred</field>
                                <field>builtYear</field>
                                <field>renovationYear</field>
                                <field>floors</field>
                                <field>noOfRooms</field>
                                <field>preferred</field>
                                <field>fullAddress</field>
                                <field>description1</field>
                                <field>description2</field>
                                <field>hotelName</field>
                                <field>address</field>
                                <field>zipCode</field>
                                <field>location</field>
                                <field>locationId</field>
                                <field>geoLocations</field>
                                <field>location1</field>
                                <field>location2</field>
                                <field>location3</field>
                                <field>cityName</field>
                                <field>cityCode</field>
                                <field>stateName</field>
                                <field>stateCode</field>
                                <field>countryName</field>
                                <field>countryCode</field>
                                <field>regionName</field>
                                <field>regionCode</field>
                                <field>attraction</field>
                                <field>amenitie</field>
                                <field>leisure</field>
                                <field>business</field>
                                <field>transportation</field>
                                <field>hotelPhone</field>
                                <field>hotelCheckIn</field>
                                <field>hotelCheckOut</field>
                                <field>minAge</field>
                                <field>rating</field>
                                <field>images</field>
                                <field>fireSafety</field>
                                <field>hotelPreference</field>
                                <field>direct</field>
                                <field>geoPoint</field>
                                <field>leftToSell</field>
                                <field>chain</field>
                                <field>lastUpdated</field>
                                <field>priority</field>
                                <roomField>name</roomField>
                                <roomField>roomInfo</roomField>
                                <roomField>roomAmenities</roomField>
                                <roomField>twin</roomField>
                            </fields>
                        </return>
                    </request>
                </customer>
                """
    
    try:
        response = requests.post(url, headers=headers, data=payload)

        if response.status_code == 200:
            try:
                root = ET.fromstring(response.text)
                hotels = root.findall('.//hotels/hotel')

                for hotel in hotels:
                    hotel_id = hotel.attrib.get('hotelid')
                    if not hotel_id:
                        continue

                    # Convert entire hotel element to nested dictionary
                    hotel_dict = xml_element_to_dict(hotel)

                    # Add root element attributes
                    hotel_dict['_root_attributes'] = root.attrib

                    # Save complete data
                    filename = os.path.join(output_dir, f"{hotel_id}.json")
                    with open(filename, 'w', encoding='utf-8') as f:
                        json.dump(hotel_dict, f, indent=4, ensure_ascii=False)

                    print(f"Saved complete data for hotel {hotel_id}")

                print(f"Processed {len(hotels)} hotels for city code {city_code}")

            except ET.ParseError as e:
                print(f"XML parsing error for city code {city_code}: {e}")
    except requests.exceptions.RequestException as e:
        print(f"Request failed for city {city_code}: {str(e)}")
        return False
    

def main():
    city_codes_file = "D:/Rokon/ofc_git/row_content_create/supplier/dotw/get_all_city_code.txt"
    base_path = "D:/content_for_hotel_json/cdn_row_collection/dotw"
    os.makedirs(base_path, exist_ok=True)

    # Read and clean city codes
    with open(city_codes_file, 'r') as file:
        original_codes = [line.strip() for line in file if line.strip()]

    remaining_codes = []
    processed_count = 0

    for city_code in original_codes:
        if process_city(city_code, base_path):
            processed_count += 1
        else:
            remaining_codes.append(city_code)

    # Update city code file with remaining codes
    with open(city_codes_file, 'w') as file:
        file.write('\n'.join(remaining_codes))

    print(f"\nProcessing complete! Successfully processed {processed_count} cities.")
    print(f"{len(remaining_codes)} cities remaining in the list.")

if __name__ == "__main__":
    main()