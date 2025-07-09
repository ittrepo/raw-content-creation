import uuid
import random
import hashlib
import base64
from datetime import datetime, timezone, timedelta
import requests
import xml.etree.ElementTree as ET
import json
from dotenv import load_dotenv
import os

load_dotenv()

def generate_password_digest(nonce, created, password):
    raw_digest = base64.b64decode(nonce) + created.encode() + password.encode()
    sha1_digest = hashlib.sha1(raw_digest).digest()
    password_digest = base64.b64encode(sha1_digest).decode()
    return password_digest

# Example usage parameters
organization = 'NMC-SAUDI'

user_id = os.getenv('AMADEUSE_USER_ID')
password = 'psC6Gh=q3qPb'
office_id = 'DMMS228XU'
duty_code = 'SU'
requestor_type = 'U'
wsap = '1ASIWAAAAAK'
url = os.getenv('AMADEUSE_LIVE_URL')
# Generate UUID
uuid_value = str(uuid.uuid4())

# Generate timestamp
t = datetime.now(timezone.utc)
micro = f"{t.microsecond:06d}"[:-3]  # Extract microseconds and format to 3 digits
date_str = t.strftime("%Y-%m-%dT%H:%M:%S") + micro + 'Z'

# Generate nonce
nonce = random.randint(10000000, 99999999)
nounce = base64.b64encode(str(nonce).encode()).decode()

# Generate password digest
pass_sha = base64.b64encode(hashlib.sha1(str(nonce).encode() + date_str.encode() + hashlib.sha1(password.encode()).digest()).digest()).decode()

# Dynamically generate valid start and end dates
start_date = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d")  # Tomorrow
end_date = (datetime.now(timezone.utc) + timedelta(days=2)).strftime("%Y-%m-%d")    # Day after tomorrow

# Location type.
latitude = '2839960'
longitude = '3653000'
# 24.3381782,45.8110997
# Construct SOAP XML request
soap_xml_request = f"""
<soap:Envelope xmlns:soap='http://schemas.xmlsoap.org/soap/envelope/'>
  <soap:Header>
    <add:MessageID xmlns:add='http://www.w3.org/2005/08/addressing'>{uuid_value}</add:MessageID>
    <add:Action xmlns:add='http://www.w3.org/2005/08/addressing'>http://webservices.amadeus.com/Hotel_MultiSingleAvailability_10.0</add:Action>
    <add:To xmlns:add='http://www.w3.org/2005/08/addressing'>{url}/1ASIWAAAAAK</add:To>
    <oas:Security xmlns:oas='http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd'>
      <oas:UsernameToken xmlns:oas1='http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd' oas1:Id='UsernameToken-1'>
        <oas:Username>{user_id}</oas:Username>
        <oas:Nonce EncodingType='http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-soap-message-security-1.0#Base64Binary'>{nounce}</oas:Nonce>
        <oas:Password Type='http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordDigest'>{pass_sha}</oas:Password>
        <oas1:Created>{date_str}</oas1:Created>
      </oas:UsernameToken>
    </oas:Security>
    <AMA_SecurityHostedUser xmlns='http://xml.amadeus.com/2010/06/Security_v1'>
      <UserID POS_Type='1' PseudoCityCode='{office_id}' AgentDutyCode='{duty_code}' RequestorType='{requestor_type}'/>
    </AMA_SecurityHostedUser>
  </soap:Header>

  <soap:Body>
<OTA_HotelAvailRQ EchoToken='MultiSingle' Version='4.000' SummaryOnly='true' AvailRatesOnly='true' OnRequestInd='true' RateRangeOnly='true' ExactMatchOnly='false' RateDetailsInd='true' SearchCacheLevel='Live' RequestedCurrency='USD'>
  <AvailRequestSegments>
      <AvailRequestSegment InfoSource='Distribution'>

          <HotelSearchCriteria>

              <Criterion ExactMatch='true'>

                 <Position Latitude='{latitude}' Longitude='{longitude}'></Position>
                  <Radius Distance='50' DistanceMeasure='DIS' UnitOfMeasureCode='2'></Radius>
                  <StayDateRange Start='{start_date}' End='{end_date}' />
                  <RoomStayCandidates>
                      <RoomStayCandidate RoomID='1' Quantity='1'>
                          <GuestCounts>
                              <GuestCount AgeQualifyingCode='10' Count='1' />
                          </GuestCounts>
                      </RoomStayCandidate>
                  </RoomStayCandidates>
              </Criterion>
          </HotelSearchCriteria>
      </AvailRequestSegment>
  </AvailRequestSegments>
</OTA_HotelAvailRQ>

  </soap:Body>
</soap:Envelope>
"""

# Prepare headers and send request
header_data = {
    "Content-Type": "text/xml",
    "SOAPAction": "http://webservices.amadeus.com/Hotel_MultiSingleAvailability_10.0"
}

response = requests.post(url=url, headers=header_data, data=soap_xml_request)

def xml_to_dict(element):
    """Recursively convert an XML element to a dictionary."""
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

if response.status_code == 200:
    # Parse XML response
    root = ET.fromstring(response.content)
    # Convert XML to dictionary
    response_dict = xml_to_dict(root)
    # Convert dictionary to JSON
    json_output = json.dumps(response_dict, indent=2)
    # print(json_output)
    
    # Save JSON to file
    output_path = "amadeus_response_1.json"  # You can change the filename as needed
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(json_output)
    print(f"JSON saved to {output_path}")
else:
    print(f"Request failed with status code: {response.status_code}")