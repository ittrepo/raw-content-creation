import pandas as pd
import requests
import xml.etree.ElementTree as ET
import time
from dotenv import load_dotenv
import os

load_dotenv()


GIATA_KEY = os.getenv("GIATA_API_key")


# Read the Excel file
df = pd.read_excel('Umrahbooking.xlsx')

df['giata_present'] = df['giata_present'].astype(str)

headers = {
    'Authorization': f'Basic {GIATA_KEY}'
}

for index, row in df.iterrows():
    ratehawk_code = row['Ratehawk Code']
    url = f"https://multicodes.giatamedia.com/webservice/rest/1.0/properties/gds/ratehawk2/{ratehawk_code}"
    
    try:
        response = requests.post(url, headers=headers)
        response.raise_for_status()  
        
        root = ET.fromstring(response.content)
        property_element = root.find('property')
        
        # Check for giataId attribute and set giata_id to that value if present
        if property_element is not None and 'giataId' in property_element.attrib:
            giata_id = property_element.attrib['giataId']
        else:
            giata_id = 'no'
            
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred for {ratehawk_code}: {http_err}")
        giata_id = 'error'
    except ET.ParseError as xml_err:
        print(f"XML parse error for {ratehawk_code}: {xml_err}")
        giata_id = 'error'
    except Exception as err:
        print(f"Other error occurred for {ratehawk_code}: {err}")
        giata_id = 'error'
    
    # Update the 'giata_present' column with the actual giata id or fallback value
    df.at[index, 'giata_present'] = giata_id
    
    df.to_excel('Umrahbooking_updated.xlsx', index=False)
    
    # Print a success message for the processed row
    print(f"Processed {ratehawk_code} successfully with giata id: {giata_id}")
    
    # Delay to avoid hitting rate limits
    time.sleep(1)

print("Processing complete. Updated file saved as 'Umrahbooking_updated.xlsx'")
