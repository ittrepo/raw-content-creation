import pandas as pd
import pycountry

# ——— Paths ———
input_path  = r"D:/Rokon/ofc_git/row_content_create/xl_check_db/webbeds/dotw_checked_output_with_varvotech_id.xlsx"
output_path = r"D:/Rokon/ofc_git/row_content_create/xl_check_db/webbeds/dotw_checked_output_with_country_iso.xlsx"

# ——— Load file ———
df = pd.read_excel(input_path)

# ——— Lookup function ———
def get_iso_code(name):
    try:
        return pycountry.countries.lookup(name).alpha_2
    except (LookupError, AttributeError):
        return "Unknown"

# ——— Apply and save ———
df["Country_ISO"] = df["Country"].astype(str).apply(get_iso_code)
df.to_excel(output_path, index=False)

print("✅ Done. New file saved to:", output_path)
