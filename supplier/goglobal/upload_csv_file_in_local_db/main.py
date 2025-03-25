import pandas as pd
from sqlalchemy import create_engine, VARCHAR, Integer, Float, String

# Database connection
DATABASE_URL = "mysql+pymysql://root:@localhost/csvdata01_02102024"
engine = create_engine(DATABASE_URL)

upload_file = "D:/Rokon/supplier_csv/goglobal/Z29nLTMxNDAw.csv"

try:
    # Load data with explicit type for giataCode
    df = pd.read_csv(
        upload_file,
        sep=';',
        encoding='utf-8',
        dtype={'giataCode': str},  # Force giataCode to be treated as string
        on_bad_lines='warn'
    )
    
    # Clean column names
    df.columns = [col.strip().replace(';', '') for col in df.columns]
    
    print("Data loaded successfully. Sample:")
    print(df.head())

except Exception as e:
    print(f"Error reading file: {e}")
    exit()

# Define SQL data types for each column
dtype_mapping = {
    'hotelCode': Integer(),
    'hotelName': String(255),
    'country': String(2),
    'latitude': Float(),
    'longitude': Float(),
    'address': String(255),
    'categoryCode': String(10),  # Changed to String to handle '3 STARS'
    'giataCode': String(20),     # Explicit string type
    'city': String(100)
}

# Upload to MySQL
try:
    df.to_sql(
        name='goglobal_master',
        con=engine,
        if_exists='replace',
        index=False,
        dtype=dtype_mapping  # Use our explicit type mapping
    )
    print("Data uploaded to MySQL successfully!")
except Exception as e:
    print(f"Database error: {e}")
finally:
    engine.dispose()