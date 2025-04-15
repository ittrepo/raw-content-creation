import pandas as pd

# Read the hotel IDs from the text file, cleaning up the lines.
with open('juniper_hotel_id_list.txt', 'r') as file:
    # Strip whitespace and any leading '|' characters, while ignoring empty lines.
    id_list = [line.strip().lstrip('|') for line in file if line.strip()]

# Read in the Excel file that contains the 'HotelCode' column.
df = pd.read_excel('Portfolio_NofShon.xlsx')

df['find'] = df['HotelCode'].apply(lambda code: 'Yes' if str(code).strip() in id_list else 'No')

# Write the updated DataFrame to a new Excel file.
df.to_excel('get_new_file.xlsx', index=False)
