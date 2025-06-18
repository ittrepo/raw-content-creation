import os, json, requests
from dotenv import load_dotenv

load_dotenv()



BASE_PATH = r"D:/content_for_hotel_json/cdn_row_collection/RNRhotel"
os.makedirs(BASE_PATH, exist_ok=True)

url = "https://api.rnrrooms.com/api-b2b/v1/lodging/innovate/hotels"
# either hard‑code or better, pull from env:
username = os.getenv("RNR_USERNAME")
password = os.getenv("RNR_PASSWORD")

resp = requests.get(url, auth=(username, password))
print("Status code:", resp.status_code)
resp.raise_for_status()

for line in resp.iter_lines(decode_unicode=True):
    if not line.strip():
        continue
    obj = json.loads(line)
    hid = obj.get("id")
    if not hid:
        continue
    fn = os.path.join(BASE_PATH, f"{hid}.json")
    with open(fn, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    print("Wrote", fn)
