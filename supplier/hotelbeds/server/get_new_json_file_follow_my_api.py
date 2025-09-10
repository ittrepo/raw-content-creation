import requests
import json
import time
from pathlib import Path

# Config
INPUT_FILE = Path("hotelbeds_new.txt") 
OUT_DIR = Path(r"D:\content_for_hotel_json\HotelInfo\hotelbeds_new_2")
URL = "https://mappingapi.innsightmap.com/hotel/details"
HEADERS = {"Content-Type": "application/json"}
TIMEOUT = 15 
MAX_RETRIES = 3
RETRY_BACKOFF = 1.5
SLEEP_BETWEEN = 0.15

OUT_DIR.mkdir(parents=True, exist_ok=True)

def read_ids(file_path: Path):
    if not file_path.exists():
        raise FileNotFoundError(f"Input file not found: {file_path}")
    with file_path.open("r", encoding="utf-8") as f:
        seen = set()
        ids = []
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            if s not in seen:
                seen.add(s)
                ids.append(s)
        return ids

def save_response_text(hotel_id: str, content: str):
    out_path = OUT_DIR / f"{hotel_id}.json"
    try:
        parsed = json.loads(content)
        with out_path.open("w", encoding="utf-8") as fw:
            json.dump(parsed, fw, ensure_ascii=False, indent=2)
    except Exception:
        with out_path.open("w", encoding="utf-8") as fw:
            fw.write(content)

def fetch_and_save(hotel_id: str):
    payload = {"supplier_code": "hotelbeds", "hotel_id": hotel_id}
    attempt = 0
    backoff = 1.0
    while attempt < MAX_RETRIES:
        try:
            resp = requests.post(URL, headers=HEADERS, json=payload, timeout=TIMEOUT)
            if resp.ok:
                save_response_text(hotel_id, resp.text)
                return True, resp.status_code, None
            else:
                if 400 <= resp.status_code < 500:
                    return False, resp.status_code, f"Client error: {resp.text[:200]}"
                attempt += 1
                time.sleep(backoff)
                backoff *= RETRY_BACKOFF
        except requests.RequestException as e:
            attempt += 1
            time.sleep(backoff)
            backoff *= RETRY_BACKOFF
            last_exc = e
    return False, None, f"Failed after {MAX_RETRIES} attempts. Last exception: {last_exc}"

def main():
    ids = read_ids(INPUT_FILE)
    print(f"Found {len(ids)} hotel ids. Saving into: {OUT_DIR}")
    summary = []
    for i, hid in enumerate(ids, start=1):
        print(f"[{i}/{len(ids)}] Fetching hotel id {hid} ...", end=" ")
        ok, status, msg = fetch_and_save(hid)
        if ok:
            print(f"saved (HTTP {status}).")
            summary.append((hid, "saved", status))
        else:
            print(f"error -> {msg}")
            summary.append((hid, "error", status if status else "N/A", str(msg)))
        time.sleep(SLEEP_BETWEEN)
    saved = sum(1 for s in summary if s[1] == "saved")
    errors = len(summary) - saved
    print("="*40)
    print(f"Done. Saved: {saved}, Errors: {errors}")
    if errors:
        print("Errors detail:")
        for item in summary:
            if item[1] != "saved":
                print(item)

if __name__ == "__main__":
    main()
