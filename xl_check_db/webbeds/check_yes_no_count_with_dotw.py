import pandas as pd

# Path to the existing Excel file
output_path = "D:/Rokon/ofc_git/row_content_create/xl_check_db/webbeds/dotw_checked_output.xlsx"

# Load the Excel file
df = pd.read_excel(output_path)

# Count the values in 'dotw_present_itt' column
counts = df["dotw_present_itt"].value_counts()

# Print summary
print("📊 Count Summary:")
print(f'✔️  Yes Present: {counts.get("Yes Present", 0)}')
print(f'❌ No: {counts.get("No", 0)}')
