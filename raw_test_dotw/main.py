import csv

def search_dotw_any(dotw_id, filename='dotw_row.txt'):
    with open(filename, newline='', encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile)
        
        for row in reader:
            # Strip quotes if any and check both columns
            dotw_val = row.get('dotw', '').strip('"')
            dotw_a_val = row.get('dotw_a', '').strip('"')
            dotw_b_val = row.get('dotw_b', '').strip('"')

            if dotw_val == str(dotw_id) or dotw_a_val == str(dotw_id) or dotw_b_val == str(dotw_id):
                result = {
                    'Id': int(row['Id']),
                    'dotw': dotw_val if dotw_val else None,
                    'dotw_a': dotw_a_val if dotw_a_val else None,
                    'dotw_b': dotw_b_val if dotw_b_val else None,
                    'CityName': row['CityName'] if row['CityName'] else None,
                    'StateName': row['StateName'] if row['StateName'] else None,
                    'CountryName': row['CountryName'] if row['CountryName'] else None,
                    'CountryCode': row['CountryCode'] if row['CountryCode'] else None,
                    'PropertyType': row['PropertyType'] if row['PropertyType'] else None,
                    'Rating': float(row['Rating']) if row['Rating'] else None
                }
                return result
    return None

# Example:
search_id = "425925"
found = search_dotw_any(search_id)
if found:
    print(found)
else:
    print(f"No entry found with dotw or dotw_a = {search_id}")
