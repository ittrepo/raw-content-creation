# Read the contents of a.txt
with open('restel_hotel_id_list.txt', 'r') as file_a:
    content_a = file_a.read()

# Read the contents of b.txt
with open('get_all_hotel_code_from_restel.txt', 'r') as file_b:
    content_b = file_b.read()

# Concatenate the contents with a separator
combined_content = f"{content_a} | {content_b}"

# Write the combined content to c.txt
with open('c.txt', 'w') as file_c:
    file_c.write(combined_content)

print("Contents have been successfully concatenated into c.txt.")
