# Read the file and process each line
with open('restel_hotel_id_list.txt', 'r') as file:
    ids = file.readlines()

# Process each ID
modified_ids = []
for hotel_id in ids:
    hotel_id = hotel_id.strip()  # Remove any leading/trailing whitespace
    if len(hotel_id) < 6:
        # Pad with leading zeros to make it 6 characters long
        hotel_id = hotel_id.zfill(6)
    modified_ids.append(hotel_id)

# Write the modified IDs back to the file
with open('restel_hotel_id_list.txt', 'w') as file:
    for hotel_id in modified_ids:
        file.write(hotel_id + '\n')

print("IDs have been updated successfully.")
