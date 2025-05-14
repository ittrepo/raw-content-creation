import pandas as pd

# File paths
excel_path = "D:/Rokon/ofc_git/row_content_create/xl_check_db/webbeds/get_new_one_no_file_tracking_data.xlsx"
txt_path = "D:/Rokon/ofc_git/row_content_create/hotel_id_count_function/all/varvotech_id_list.txt"
output_path = "D:/Rokon/ofc_git/row_content_create/xl_check_db/webbeds/dotw_checked_output_with_varvotech_id.xlsx"

# Load Excel file
df = pd.read_excel(excel_path)

# Load text file and remove whitespace/newlines
with open(txt_path, "r") as f:
    hotel_ids = set(line.strip() for line in f if line.strip())

# Initialize new column
status_list = []
for code in df["Vervotech_HotelCode"].astype(str):
    if code in hotel_ids:
        status_list.append("Yes Present")
    else:
        status_list.append("No")

# Assign the result to a new column
df["vervotech_present_itt"] = status_list

# Count values
count_summary = df["vervotech_present_itt"].value_counts()

# Print counts
print("📊 Vervotech ID Check Summary:")
print(f"✔️  Yes Present: {count_summary.get('Yes Present', 0)}")
print(f"❌ No: {count_summary.get('No', 0)}")

# Save to a new Excel file
df.to_excel(output_path, index=False)

print("✅ Done. File saved to:", output_path)
