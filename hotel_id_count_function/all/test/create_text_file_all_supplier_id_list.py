from sqlalchemy import  create_engine, text
from dotenv import load_dotenv
import os
import pandas as pd
import numpy as np


load_dotenv()


db_host = os.getenv('DB_HOST')
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')

table = "global_hotel_mapping_copy"


connection_url = f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}"
engine = create_engine(connection_url)


provider_mappings = {
    "hotelbeds": ["hotelbeds", "hotelbeds_a", "hotelbeds_b", "hotelbeds_c", "hotelbeds_d", "hotelbeds_e"]
}


def generate_hotel_id_files(provider_mappings, engine, table_name):
    for supplier, columns in provider_mappings.items():
        if not columns:
            continue
        
        selected_columns = ", ".join(columns)
        where_clause = " OR ".join([f"{col} IS NOT NULL AND {col} != ''" for col in columns])
        query = f"SELECT {selected_columns} FROM {table_name} WHERE {where_clause};"
        
        try:
            df = pd.read_sql(text(query), engine)
            
            hotel_ids = set()
            for col in columns:
                non_empty = df[col].astype(str).str.strip().replace(r'^\s*$', np.nan, regex=True).dropna()
                hotel_ids.update(non_empty.unique())
            
            if hotel_ids:
                # Split IDs into numeric and non-numeric groups
                numeric_ids = []
                non_numeric_ids = []
                for hotel_id in hotel_ids:
                    if hotel_id.isdigit():
                        numeric_ids.append(int(hotel_id))  
                    else:
                        non_numeric_ids.append(hotel_id)
                
                # Sort numeric IDs as integers, non-numeric as strings
                sorted_numeric = sorted(numeric_ids)
                sorted_non_numeric = sorted(non_numeric_ids)
                
                # Combine results and convert back to strings
                sorted_ids = [str(id) for id in sorted_numeric] + sorted_non_numeric
                
                filename = f"{supplier}_hotel_id_list.txt"
                with open(filename, 'w') as f:
                    f.write("\n".join(sorted_ids) + "\n")
                print(f"Generated {filename} with {len(sorted_ids)} unique hotel IDs.")
            else:
                print(f"No hotel IDs found for {supplier}.")
        
        except Exception as e:
            print(f"Error processing {supplier}: {e}")

# Execute the function
generate_hotel_id_files(provider_mappings, engine, table)
