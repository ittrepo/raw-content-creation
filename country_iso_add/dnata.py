import os
import json
import pycountry
from collections import OrderedDict

def get_country_codes(country_name):
    country = pycountry.countries.get(name=country_name)
    if country:
        return {
            'alpha_2': country.alpha_2
        }
    else:
        return None

def update_json_files(directory):
    for filename in os.listdir(directory):
        if filename.endswith('.json'):
            file_path = os.path.join(directory, filename)
            try:
                with open(file_path, 'r', encoding='utf-8') as file:
                    data = json.load(file, object_pairs_hook=OrderedDict)

                # Check if 'country' key exists
                if 'country' in data:
                    country_name = data['country']
                    country_info = get_country_codes(country_name)
                    if country_info:
                        # Create a new ordered dictionary with 'country_code' inserted
                        new_data = OrderedDict()
                        for key, value in data.items():
                            new_data[key] = value
                            if key == 'country':
                                new_data['country_code'] = country_info['alpha_2']

                        print(f"Successfully added country_code to {filename}")

                        # Update the original data
                        data = new_data
                    else:
                        print(f"Country not found for {country_name} in file {filename}")
                else:
                    print(f"Country key not found in file {filename}")

                with open(file_path, 'w', encoding='utf-8') as file:
                    json.dump(data, file, indent=4)

            except Exception as e:
                print(f"Error processing file {filename}: {e}")

# Specify the directory containing the JSON files
directory_path = r'D:\content_for_hotel_json\cdn_row_collection\dnata'
update_json_files(directory_path)
