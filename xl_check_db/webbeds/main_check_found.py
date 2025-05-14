import pandas as pd

# File paths
excel_path = "D:/Rokon/ofc_git/row_content_create/xl_check_db/webbeds/dotw_get_data_from_supplier_raw.xlsx"
txt_path = "D:/Rokon/ofc_git/row_content_create/hotel_id_count_function/all/dotw_hotel_id_list.txt"
output_path = "D:/Rokon/ofc_git/row_content_create/xl_check_db/webbeds/dotw_checked_output.xlsx"

# Load Excel file
df = pd.read_excel(excel_path)

# Load text file and remove whitespace/newlines
with open(txt_path, "r") as f:
    hotel_ids = set(line.strip() for line in f if line.strip())

# Initialize new column
status_list = []
for code in df["DOTW_HotelCode"].astype(str):
    if code in hotel_ids:
        print("Find")
        status_list.append("Yes Present")
    else:
        status_list.append("No")

# Assign the result to a new column
df["dotw_present_itt"] = status_list

# Save to a new Excel file
df.to_excel(output_path, index=False)

print("✅ Done. File saved to:", output_path)
