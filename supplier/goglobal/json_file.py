import xml.etree.ElementTree as ET
from html import unescape
import json
import re

def fix_truncated_xml(xml_str):
    """
    Heuristic to append missing closing tags if the XML appears truncated.
    This is a hack and assumes a specific structure.
    """
    # If there's an open <Description><![CDATA[ without its closing sequence, fix it:
    if "<Description><![CDATA[" in xml_str and "]]></Description>" not in xml_str:
        xml_str += "]]></Description>"
    
    # Ensure the Main element is closed
    if "</Main>" not in xml_str:
        xml_str += "</Main>"
    
    # Ensure the Root element is closed
    if "</Root>" not in xml_str:
        xml_str += "</Root>"
    
    return xml_str

# Sample response (replace with actual response.text)
response_text = '''<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
    <soap:Body>
        <MakeRequestResponse xmlns="http://www.goglobal.travel/">
            <MakeRequestResult>&lt;Root&gt;&lt;Header&gt;&lt;Agency&gt;149548&lt;/Agency&gt;&lt;User&gt;NOFSHONXMLTEST&lt;/User&gt;&lt;Password&gt;&lt;/Password&gt;&lt;Operation&gt;HOTEL_INFO_RESPONSE&lt;/Operation&gt;&lt;OperationType&gt;Response&lt;/OperationType&gt;&lt;/Header&gt;&lt;Main&gt;&lt;HotelSearchCode&gt;&lt;![CDATA[]]&gt;&lt;/HotelSearchCode&gt;&lt;HotelName&gt;&lt;![CDATA[DoubleTree by Hilton Harrogate Majestic Hotel &amp; Spa]]&gt;&lt;/HotelName&gt;&lt;HotelId&gt;&lt;![CDATA[3133]]&gt;&lt;/HotelId&gt;&lt;Address&gt;&lt;![CDATA[Ripon Road, HG1 2HU, Harrogate, UNITED KINGDOM]]&gt;&lt;/Address&gt;&lt;CityCode&gt;&lt;![CDATA[779]]&gt;&lt;/CityCode&gt;&lt;Phone&gt;&lt;![CDATA[]]&gt;&lt;/Phone&gt;&lt;Fax&gt;&lt;![CDATA[]]&gt;&lt;/Fax&gt;&lt;Category&gt;&lt;![CDATA[4]]&gt;&lt;/Category&gt;&lt;Description&gt;&lt;![CDATA[Set in gardens...<BR />...]]&gt;</MakeRequestResult>
        </MakeRequestResponse>
    </soap:Body>
</soap:Envelope>'''

# Parse the SOAP response XML
soap_root = ET.fromstring(response_text)
namespace = {
    'soap': 'http://www.w3.org/2003/05/soap-envelope',
    'ns': 'http://www.goglobal.travel/'
}
make_request_result = soap_root.find('.//soap:Body/ns:MakeRequestResponse/ns:MakeRequestResult', namespace).text

# Unescape HTML entities to get the inner XML
unescaped_xml = unescape(make_request_result)

print("Unescaped XML before fix:")
print(unescaped_xml)

# Attempt to fix the truncated XML
fixed_xml = fix_truncated_xml(unescaped_xml)
print("\nFixed XML:")
print(fixed_xml)

# Parse the (hopefully fixed) inner XML
try:
    inner_root = ET.fromstring(fixed_xml)
except ET.ParseError as e:
    print(f"XML Parse Error: {e}")
    print("The inner XML still appears to be malformed. Please check the source.")
    exit(1)

# Function to safely get element text (returns an empty string if missing)
def get_text(element, path):
    elem = element.find(path)
    return elem.text if elem is not None and elem.text is not None else ""

# Build the result dictionary, ensuring every key gets a value
result = {
    'Header': {},
    'Main': {}
}

# Process Header
header = inner_root.find('Header')
result['Header']['Agency'] = get_text(header, 'Agency')
result['Header']['User'] = get_text(header, 'User')
result['Header']['Password'] = get_text(header, 'Password')
result['Header']['Operation'] = get_text(header, 'Operation')
result['Header']['OperationType'] = get_text(header, 'OperationType')

# Process Main
main = inner_root.find('Main')
result['Main']['HotelSearchCode'] = get_text(main, 'HotelSearchCode')
result['Main']['HotelName'] = get_text(main, 'HotelName')
result['Main']['HotelId'] = get_text(main, 'HotelId')
result['Main']['Address'] = get_text(main, 'Address')
result['Main']['CityCode'] = get_text(main, 'CityCode')
result['Main']['Phone'] = get_text(main, 'Phone')
result['Main']['Fax'] = get_text(main, 'Fax')
result['Main']['Category'] = get_text(main, 'Category')
result['Main']['Description'] = get_text(main, 'Description')

# Process HotelFacilities
hotel_facilities = get_text(main, 'HotelFacilities')
result['Main']['HotelFacilities'] = [facility.strip() for facility in hotel_facilities.split('<BR />') if facility.strip()]

# Process RoomFacilities using regex to extract room type and facilities
room_facilities_text = get_text(main, 'RoomFacilities')
room_facilities = []
pattern = re.compile(r'<b>Room Type:\s*(.*?)\s*</b>(.*?)(?=<b>|$)', re.DOTALL)
matches = pattern.findall(room_facilities_text)
for room_type, facilities_block in matches:
    facilities = [f.strip() for f in facilities_block.split('<BR />') if f.strip()]
    room_facilities.append({
        'room_type': room_type.strip(),
        'facilities': facilities
    })
result['Main']['RoomFacilities'] = room_facilities

# Process Pictures
result['Main']['Pictures'] = []
pictures_element = main.find('Pictures')
if pictures_element is not None:
    for picture in pictures_element.findall('Picture'):
        desc = picture.get('Description', "")
        url = picture.text.strip() if picture.text else ""
        result['Main']['Pictures'].append({
            'description': desc,
            'url': url
        })

# Convert the result dictionary to JSON and print it
json_output = json.dumps(result, indent=2, ensure_ascii=False)
print("\nJSON Output:")
print(json_output)
