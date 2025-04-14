import os
import json

# Define folder path and output file
folder_path = r'D:/content_for_hotel_json/cdn_row_collection/tbo'
empty_file_log = 'find_id.txt'
error_500_log = '500_get.txt'

# Clear output files first
open(empty_file_log, 'w').close()
open(error_500_log, 'w').close()

# Go through all JSON files
for filename in os.listdir(folder_path):
    if filename.endswith('.json'):
        file_path = os.path.join(folder_path, filename)
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            file_id = os.path.splitext(filename)[0]

            # Check for empty JSON
            if data == {}:
                os.remove(file_path)
                with open(empty_file_log, 'a') as empty_log:
                    empty_log.write(file_id + '\n')

            # Check for 500 status
            elif (
                isinstance(data, dict)
                and "Status" in data
                and data["Status"].get("Code") == 500
                and data["Status"].get("Description") == "Not able to fetch Hotel details"
            ):
                with open(error_500_log, 'a') as error_log:
                    error_log.write(file_id + '\n')
            print(f"Processed {filename} successfully.")
        except Exception as e:
            print(f"Skipping {filename}: {e}")