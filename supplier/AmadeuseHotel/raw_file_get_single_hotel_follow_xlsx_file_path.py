import os
import requests
import uuid
import base64
import hashlib
import datetime
import random
import json
from dotenv import load_dotenv
import xml.etree.ElementTree as ET
import openpyxl


load_dotenv()

# ---------- Configuration ---------- #
organization = 'NMC-SAUDI'
user_id = os.getenv('AMADEUSE_USER_ID')
password = 'psC6Gh=q3qPb'
office_id = 'DMMS228XU'
duty_code = 'SU'
requestor_type = 'U'
soap_action = 'http://webservices.amadeus.com/OTA_HotelDescriptiveInfoRQ_07.1_1A2007A'
wsap = '1ASIWAAAAAK'
url = os.getenv('AMADEUSE_LIVE_URL')


hotel_id_xlsx = r"D:\Rokon\ofc_git\row_content_create\supplier\amadeushotel\amadeuse_hotel_list.xlsx"
output_dir_for_single = r"D:\content_for_hotel_json\cdn_row_collection\amadeushotel\single_hotel_data_lint_2"
os.makedirs(output_dir_for_single, exist_ok=True)

def generate_uuid():
    return str(uuid.uuid4())

def get_timestamp():
    now = datetime.datetime.utcnow()
    micro = f"{now.microsecond // 1000:03d}"
    timestamp = now.strftime('%Y-%m-%dT%H:%M:%S') + micro + 'Z'
    return timestamp

def generate_nonce():
    return base64.b64encode(str(random.randint(10000000, 99999999)).encode()).decode()

def generate_password_digest(nonce_b64, created, password):
    nonce_bytes = base64.b64decode(nonce_b64)
    sha1_password = hashlib.sha1(password.encode('utf-8')).digest()
    digest = hashlib.sha1(nonce_bytes + created.encode() + sha1_password).digest()
    return base64.b64encode(digest).decode()


def remove_namespace(obj):
    if isinstance(obj, dict):
        new_obj = {}
        for k, v in obj.items():
            # Remove the namespace from the key if present
            if isinstance(k, str) and k.startswith("{http://www.opentravel.org/OTA/2003/05}"):
                new_key = k.replace("{http://www.opentravel.org/OTA/2003/05}", "")
            else:
                new_key = k
            new_obj[new_key] = remove_namespace(v)
        return new_obj
    elif isinstance(obj, list):
        return [remove_namespace(item) for item in obj]
    else:
        return obj
    

def xml_to_dict(element):
    result = {}
    if element.attrib:
        result.update(element.attrib)
    if element.text and element.text.strip():
        if result:
            result['text'] = element.text.strip()
        else:
            result = element.text.strip()
    for child in element:
        child_data = xml_to_dict(child)
        if child.tag in result:
            if isinstance(result[child.tag], list):
                result[child.tag].append(child_data)
            else:
                result[child.tag] = [result[child.tag], child_data]
        else:
            result[child.tag] = child_data
    return result


# Read hotel codes from Excel
wb = openpyxl.load_workbook(hotel_id_xlsx)
ws = wb.active
hotel_codes = []
for row in ws.iter_rows(min_row=2, values_only=True):
    code = row[0]
    if code:
        hotel_codes.append(str(code).strip()) 

for hotel_code in hotel_codes:
    out_path = os.path.join(output_dir_for_single, f"{hotel_code}.json")
    if os.path.exists(out_path):
        print(f"⏩ Skipping {hotel_code}, file already exists.")
        continue

    print(f"Processing hotel_code: {hotel_code}")

    uuid_val = generate_uuid()
    timestamp = get_timestamp()
    nonce = generate_nonce()
    password_digest = generate_password_digest(nonce, timestamp, password)

    # ---------- Create SOAP Payload ---------- #
    soap_payload = f"""<?xml version="1.0" encoding="UTF-8"?>
                        <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
                        <soap:Header>
                            <add:MessageID xmlns:add="http://www.w3.org/2005/08/addressing">{uuid_val}</add:MessageID>
                            <add:Action xmlns:add="http://www.w3.org/2005/08/addressing">{soap_action}</add:Action>
                            <add:To xmlns:add="http://www.w3.org/2005/08/addressing">{url}/{wsap}</add:To>
                            <oas:Security xmlns:oas="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
                            <oas:UsernameToken xmlns:oas1="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd" oas1:Id="UsernameToken-1">
                                <oas:Username>{user_id}</oas:Username>
                                <oas:Nonce EncodingType="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-soap-message-security-1.0#Base64Binary">{nonce}</oas:Nonce>
                                <oas:Password Type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordDigest">{password_digest}</oas:Password>
                                <oas1:Created>{timestamp}</oas1:Created>
                            </oas:UsernameToken>
                            </oas:Security>
                            <AMA_SecurityHostedUser xmlns="http://xml.amadeus.com/2010/06/Security_v1">
                            <UserID POS_Type="1" PseudoCityCode="{office_id}" AgentDutyCode="{duty_code}" RequestorType="{requestor_type}"/>
                            </AMA_SecurityHostedUser>
                        </soap:Header>
                        <soap:Body>
                            <OTA_HotelDescriptiveInfoRQ EchoToken="withParsing" Version="6.001" PrimaryLangID="en">
                            <HotelDescriptiveInfos>
                                <HotelDescriptiveInfo HotelCode="{hotel_code}">
                                <HotelInfo SendData="true"/>
                                <FacilityInfo SendGuestRooms="true" SendMeetingRooms="true" SendRestaurants="true"/>
                                <Policies SendPolicies="true"/>
                                <AreaInfo SendAttractions="true" SendRefPoints="true" SendRecreations="true"/>
                                <AffiliationInfo SendAwards="true" SendLoyalPrograms="false"/>
                                <ContactInfo SendData="true"/>
                                <MultimediaObjects SendData="true"/>
                                <ContentInfos>
                                    <ContentInfo Name="SecureMultimediaURLs"/>
                                </ContentInfos>
                                </HotelDescriptiveInfo>
                            </HotelDescriptiveInfos>
                            </OTA_HotelDescriptiveInfoRQ>
                        </soap:Body>
                        </soap:Envelope>"""

    headers = {
        'Content-Type': 'text/xml',
        'SOAPAction': soap_action,
    }

    try:
        response = requests.post(url, data=soap_payload.strip(), headers=headers, timeout=60)
        if response.status_code == 200:
            root = ET.fromstring(response.content)
            response_dict = xml_to_dict(root)
            response_dict = remove_namespace(response_dict)
            json_output = json.dumps(response_dict, indent=2)
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(json_output)
            print(f"✅ Saved {out_path}")
        else:
            print(f"❌ Failed for {hotel_code}: HTTP {response.status_code}")
    except Exception as e:
        print(f"❌ Exception for {hotel_code}: {e}")