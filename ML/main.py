import os
import json
import pandas as pd
import numpy as np
import faiss
from sqlalchemy import create_engine
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.decomposition import TruncatedSVD
from scipy.sparse import hstack
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()

# Get database credentials from environment variables
db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')

# Database connection string
DATABASE_URI = f'mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}'

# Connect to the database
engine = create_engine(DATABASE_URI)

# Load the JSON file
json_path = r'D:\content_for_hotel_json\HotelInfo\dnata\95210.json'
with open(json_path, 'r') as file:
    hotel_data = json.load(file)

# Convert empty strings to None in the JSON data
hotel_data = {k: (v if v != "" else None) for k, v in hotel_data.items()}

# Query the database to get the existing hotel data
query = "SELECT Id, Name, Latitude, Longitude, AddressLine1, AddressLine2, CityName FROM global_hotel_mapping"
df = pd.read_sql(query, engine)

# Fill NaN values with an empty string for text columns
for col in ['Name', 'AddressLine1', 'AddressLine2', 'CityName']:
    df[col] = df[col].fillna('')

# Impute missing values in numeric columns
imputer = SimpleImputer(strategy='mean')
df[['Latitude', 'Longitude']] = imputer.fit_transform(df[['Latitude', 'Longitude']])

# Feature engineering
vectorizer = TfidfVectorizer()
X_text = vectorizer.fit_transform(df['Name'] + " " + df['AddressLine1'] + " " + df['AddressLine2'] + " " + df['CityName'])

# Reduce dimensionality of text data
svd = TruncatedSVD(n_components=50)  # Reduce the number of components further
X_text_reduced = svd.fit_transform(X_text)

scaler = StandardScaler()
X_numeric = scaler.fit_transform(df[['Latitude', 'Longitude']])

# Use hstack from scipy.sparse to concatenate the matrices
X = np.hstack([X_text_reduced, X_numeric])

# Convert to float32 for Faiss
X = X.astype('float32')

# Build the Faiss index with approximate search
dimension = X.shape[1]
index = faiss.IndexHNSWFlat(dimension, 16)  # Use HNSW for approximate search
index.add(X)

# Function to predict ID
def predict_id(name, latitude, longitude, address1, address2, city):
    # Convert None values to empty strings
    name = str(name) if name is not None else ""
    address1 = str(address1) if address1 is not None else ""
    address2 = str(address2) if address2 is not None else ""
    city = str(city) if city is not None else ""

    input_text = vectorizer.transform([name + " " + address1 + " " + address2 + " " + city])
    input_text_reduced = svd.transform(input_text)
    input_numeric = scaler.transform([[latitude, longitude]])
    input_features = np.hstack([input_text_reduced, input_numeric]).astype('float32')

    distances, indices = index.search(input_features, k=1)

    if distances[0][0] < 0.5:  # Threshold for similarity
        return df.iloc[indices[0][0]]['Id']
    else:
        return "Cannot find"

# Predict the ID for the hotel in the JSON file
input_data = {
    'name': hotel_data['name'],
    'latitude': float(hotel_data['latitude']),
    'longitude': float(hotel_data['longitude']),
    'address1': hotel_data['address'],
    'address2': hotel_data['address2'] if hotel_data.get('address2') else "",  # Use address2 if available
    'city': hotel_data['city']
}

print(predict_id(**input_data))
