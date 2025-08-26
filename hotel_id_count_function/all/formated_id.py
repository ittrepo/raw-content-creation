# Read from the file
with open("tbohotel_hotel_id_list.txt", "r") as f:
    lines = f.readlines()

# Add commas
formatted_lines = [line.strip() + "," for line in lines if line.strip()]

# Write back to the file (or new file)
with open("tbohotel_hotel_id_list_formatted.txt", "w") as f:
    f.write("\n".join(formatted_lines))
