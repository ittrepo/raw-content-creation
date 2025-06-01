import os
import json
import urllib.parse
import xmltodict
import requests
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()

# Constants
HOTEL_ID_LIST = "D:/Rokon/ofc_git/row_content_create/hotel_id_count_function/restel/restel_hotel_id_list.txt"
TRACKING_FILE = "D:/Rokon/ofc_git/row_content_create/hotel_id_count_function/restel/restel_tracking_file.txt"
SUCCESS_FILE = "D:/Rokon/ofc_git/row_content_create/hotel_id_count_function/restel/successful_done_hotel_id_list.txt"
NOT_FOUND_FILE = "D:/Rokon/ofc_git/row_content_create/hotel_id_count_function/restel/restel_not_found.txt"
BASE_PATH = "D:/content_for_hotel_json/cdn_row_collection/restel"
MAX_WORKERS = 5

def get_supplier_own_raw_data(hotel_id):
    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
    <peticion><tipo>15</tipo><nombre>Servicio de información de hotel</nombre>
      <agencia>Agencia prueba</agencia><parametros>
        <codigo>{hotel_id}</codigo><idioma>2</idioma>
      </parametros>
    </peticion>"""
    url = (
      "http://xml.hotelresb2b.com/xml/listen_xml.jsp"
      "?codigousu=ZVYE&clausu=xml514142&afiliacio=RS&secacc=151003"
      f"&xml={urllib.parse.quote(xml)}"
    )
    r = requests.get(url, headers={'Cookie':'JSESSIONID=aaaodjlEZaLhM_vAad2xz'}, timeout=10)
    if r.status_code == 200:
        try:
            return xmltodict.parse(r.text)
        except Exception:
            return None
    return None

def save_json(raw_path, hotel_id):
    data = get_supplier_own_raw_data(hotel_id)
    status = "success" if data is not None else "not_found"
    if data is None:
        data = {}
    out = os.path.join(raw_path, f"{hotel_id}.json")
    try:
        with open(out, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except:
        status = "not_found"
    return status

def initialize_tracking_file():
    if not os.path.exists(TRACKING_FILE):
        with open(HOTEL_ID_LIST, "r", encoding="utf-8") as src, \
             open(TRACKING_FILE, "w", encoding="utf-8") as dst:
            dst.writelines(src.readlines())

def read_tracking_file():
    if not os.path.exists(TRACKING_FILE):
        return []
    with open(TRACKING_FILE, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]

def write_tracking_file(remaining):
    with open(TRACKING_FILE, "w", encoding="utf-8") as f:
        f.writelines(f"{hid}\n" for hid in remaining)

def append_file(path, hotel_id):
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"{hotel_id}\n")

def process_single(hotel_id):
    """ Returns (hotel_id, status) """
    out = os.path.join(BASE_PATH, f"{hotel_id}.json")
    if os.path.exists(out):
        return hotel_id, "skipped"
    status = save_json(BASE_PATH, hotel_id)
    return hotel_id, status

def process_hotels():
    os.makedirs(BASE_PATH, exist_ok=True)
    initialize_tracking_file()
    hotel_ids = read_tracking_file()
    if not hotel_ids:
        print("No hotel IDs left to process.")
        return

    processed = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
        futures = {exe.submit(process_single, hid): hid for hid in hotel_ids}
        for fut in as_completed(futures):
            hid, status = fut.result()
            processed.append(hid)
            if status in ("success", "skipped"):
                append_file(SUCCESS_FILE, hid)
                print(f"✅ {hid}: {status}")
            else:
                append_file(NOT_FOUND_FILE, hid)
                print(f"⚠️  {hid}: not found")

    remaining = [hid for hid in hotel_ids if hid not in processed]
    write_tracking_file(remaining)
    print(f"Cycle complete — {len(processed)} processed, {len(remaining)} remaining.")

if __name__ == "__main__":
    while True:
        process_hotels()
        if not read_tracking_file():
            print("✅ All done — exiting.")
            break
