import pandas as pd

# Read the hotel IDs from the text file.
with open('ratehawkhotel_hotel_id_list.txt', 'r') as file:
    # Read all lines and strip whitespace and any leading '|' characters.
    id_list = [line.strip().lstrip('|') for line in file if line.strip()]

# Load the Excel file.
df = pd.read_excel('Umrahbooking.xlsx')

# Update 'giata_present' based on whether 'Ratehawk Code' is in the id list.
df['giata_present'] = df['Ratehawk Code'].apply(lambda code: 'Yes' if str(code).strip() in id_list else 'No')

# Optionally, save the updated DataFrame to a new Excel file.
df.to_excel('Umrahbooking_updated_2.xlsx', index=False)

print("Update complete. Check 'Umrahbooking_updated_2.xlsx' for the results.")
