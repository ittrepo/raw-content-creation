import pandas as pd

# Load the existing Excel file
input_path = "D:/Rokon/ofc_git/row_content_create/xl_check_db/webbeds/dotw_checked_output.xlsx"
output_no_file = "D:/Rokon/ofc_git/row_content_create/xl_check_db/webbeds/get_new_one_no_file_tracking_data.xlsx"

# Read the Excel file
df = pd.read_excel(input_path)

# Filter rows where dotw_present_itt is 'No'
df_no = df[df["dotw_present_itt"] == "No"]

# Save to a new Excel file
df_no.to_excel(output_no_file, index=False)

print(f"✅ Done. 'No' values saved to: {output_no_file}")
