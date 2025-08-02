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
from concurrent.futures import ThreadPoolExecutor

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
load_dotenv()
# output_dir = r"D:\content_for_hotel_json\cdn_row_collection\amadeuse\single_hotel_data"
output_dir = r"/var/www/Storage-Contents/Hotel-Supplier-Raw-Contents/amadeushotel"
hotel_id_file = "amadeus_hotel_id_list.txt"
os.makedirs(output_dir, exist_ok=True)

organization = 'NMC-SAUDI'
user_id = os.getenv('AMADEUSE_USER_ID')
password = 'psC6Gh=q3qPb'
office_id = 'DMMS228XU'
duty_code = 'SU'
requestor_type = 'U'
soap_action = 'http://webservices.amadeus.com/OTA_HotelDescriptiveInfoRQ_07.1_1A2007A'
wsap = '1ASIWAAAAAK'
url = os.getenv('AMADEUSE_LIVE_URL')

# ─────────────────────────────────────────────
# UTILITY FUNCTIONS
# ─────────────────────────────────────────────
def generate_uuid():
    return str(uuid.uuid4())

def get_timestamp():
    now = datetime.datetime.utcnow()
    micro = f"{now.microsecond // 1000:03d}"
    return now.strftime('%Y-%m-%dT%H:%M:%S') + micro + 'Z'

def generate_nonce():
    return base64.b64encode(str(random.randint(10000000, 99999999)).encode()).decode()

def generate_password_digest(nonce_b64, created, password):
    nonce_bytes = base64.b64decode(nonce_b64)
    sha1_password = hashlib.sha1(password.encode('utf-8')).digest()
    digest = hashlib.sha1(nonce_bytes + created.encode() + sha1_password).digest()
    return base64.b64encode(digest).decode()

def remove_namespace(obj):
    if isinstance(obj, dict):
        return {k.split('}', 1)[-1] if '}' in k else k: remove_namespace(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [remove_namespace(item) for item in obj]
    return obj

def xml_to_dict(element):
    result = {}
    if element.attrib:
        result.update(element.attrib)
    if element.text and element.text.strip():
        result['text'] = element.text.strip()
    for child in element:
        child_data = xml_to_dict(child)
        result.setdefault(child.tag, []).append(child_data) if child.tag in result else result.update({child.tag: child_data})
    return result

# ─────────────────────────────────────────────
# MAIN WORKER FUNCTION
# ─────────────────────────────────────────────
def fetch_hotel_data(hotel_code):
    out_path = os.path.join(output_dir, f"{hotel_code}.json")
    if os.path.exists(out_path):
        print(f"⏩ Skipping {hotel_code}, file already exists.")
        return

    print(f"🔄 Processing hotel_code: {hotel_code}")

    try:
        uuid_val = generate_uuid()
        timestamp = get_timestamp()
        nonce = generate_nonce()
        password_digest = generate_password_digest(nonce, timestamp, password)

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

        response = requests.post(url, data=soap_payload.strip(), headers=headers, timeout=60)

        if response.status_code == 200:
            root = ET.fromstring(response.content)
            response_dict = remove_namespace(xml_to_dict(root))
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(response_dict, f, indent=2)
            print(f"✅ Saved: {out_path}")
        else:
            print(f"❌ Failed {hotel_code}: HTTP {response.status_code}")
    except Exception as e:
        print(f"❌ Exception {hotel_code}: {e}")

# ─────────────────────────────────────────────
# EXECUTION
# ─────────────────────────────────────────────
if __name__ == "__main__":
    with open(hotel_id_file, "r") as file:
        hotel_ids = [line.strip() for line in file if line.strip()]

    print(f"📦 Total hotels to process: {len(hotel_ids)}")

    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(fetch_hotel_data, hotel_ids)
