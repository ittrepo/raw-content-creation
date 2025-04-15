import os
import xml.etree.ElementTree as ET
import requests
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database configuration
db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_pass = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')

# Create SQLAlchemy engine
connection_string = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}"
engine = create_engine(
    connection_string,
    pool_recycle=1800,
    pool_pre_ping=True,
    connect_args={"connect_timeout": 30}
)

# Create table if not exists
def create_table():
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS master_dotw (
                id INT PRIMARY KEY AUTO_INCREMENT,
                city_name VARCHAR(255),
                city_code INT,
                country_name VARCHAR(255),
                country_code INT,
                UNIQUE KEY unique_city (city_code)
            )
        """))
        conn.commit()

# Process XML and save to database
def process_xml(xml_data):
    root = ET.fromstring(xml_data)
    
    cities = []
    for city in root.findall('.//city'):
        city_data = {
            'city_name': city.find('name').text.strip(),
            'city_code': int(city.find('code').text),
            'country_name': city.find('countryName').text.strip(),
            'country_code': int(city.find('countryCode').text)
        }
        cities.append(city_data)
    
    if cities:
        with engine.connect() as conn:
            # Using INSERT IGNORE to avoid duplicate entries
            stmt = text("""
                INSERT IGNORE INTO master_dotw 
                (city_name, city_code, country_name, country_code)
                VALUES (:city_name, :city_code, :country_name, :country_code)
            """)
            conn.execute(stmt, cities)
            conn.commit()
            print(f"Inserted/Updated {len(cities)} cities")

# Main processing function
def process_countries():
    create_table()
    
    # Read country list
    with open('get_all_country_name.txt', 'r') as f:
        countries = [line.strip() for line in f if line.strip()]
    
    for country in countries:
        print(f"Processing country: {country}")
        
        payload = f"""
        <customer>  
            <username>kam786</username>  
            <password>98aa96f33fd167e34910a1ee3727d2e9</password>  
            <id>2050945</id>  
            <source>1</source>
            <request command="getallcities">  
                <return>  
                    <filters>  
                           <countryName>{country}</countryName>  
                    </filters>  
                    <fields>  
                        <field>countryName</field>  
                        <field>countryCode</field>  
                    </fields>  
                </return>  
            </request>  
        </customer>
        """
        
        headers = {
            'Content-Type': 'application/xml',
            'Cookie': 'PHPSESSID=3e62394000d0589af852105f9d7cb26e'
        }
        
        try:
            response = requests.post(
                "https://www.xmldev.dotwconnect.com/gatewayV4.dotw",
                headers=headers,
                data=payload
            )
            response.raise_for_status()
            
            if response.text:
                process_xml(response.text)
            else:
                print(f"No data for {country}")
                
        except requests.exceptions.HTTPError as e:
            print(f"Error fetching data for {country}: {str(e)}")
        except Exception as e:
            print(f"General error processing {country}: {str(e)}")

if __name__ == "__main__":
    process_countries()