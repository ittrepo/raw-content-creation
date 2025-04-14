import pandas as pd

# Load the updated Excel file.
df = pd.read_excel('Umrahbooking_updated_2.xlsx')

# Filter rows where 'giata_present' is 'No'
df_no = df[df['giata_present'] == 'No']

# Count how many rows have 'No'
count_no = len(df_no)
print(f"Number of rows with 'No': {count_no}")

# Save the filtered DataFrame to a new Excel file.
df_no.to_excel('Umrahbooking_updated_only_no.xlsx', index=False)
