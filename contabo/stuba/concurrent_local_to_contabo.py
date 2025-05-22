import paramiko
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# Local and remote directory paths
local_json_dir = r'D:\content_for_hotel_json\HotelInfo\stuba_travelGX'
local_list_file = r'D:\Rokon\ofc_git\row_content_create\contabo\stuba\stuba_Gtx_hotel.txt'
remote_dir = '/var/www/hoteljson.gtrsystem.com/stuba/'

# SSH connection details
HOST = os.getenv('CONTABO_HOST_NAME')
USER = os.getenv('CONTABO_USERNAME')
PASS = os.getenv('CONTABO_PASSWORD')

def upload_file(json_name):
    """Upload a single JSON, skipping if already on remote, and return status."""
    local_json = os.path.join(local_json_dir, f"{json_name}.json")
    remote_json = os.path.join(remote_dir, f"{json_name}.json")
    
    if not os.path.exists(local_json):
        return (json_name, False, f"⚠️ {json_name}: local file not found, skipping.")

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(HOST, username=USER, password=PASS)
    sftp = ssh.open_sftp()
    
    try:
        # 👉 check remote existence
        try:
            sftp.stat(remote_json)
            # file exists on remote, so we skip uploading
            return (json_name, True, f"⏭️ {json_name}: already on remote, skipping.")
        except IOError:
            # stat failed → file not present → proceed to upload
            pass

        sftp.put(local_json, remote_json)
        return (json_name, True, f"✅ {json_name}: uploaded.")
    except Exception as e:
        return (json_name, False, f"❌ {json_name}: upload failed ({e}).")
    finally:
        sftp.close()
        ssh.close()


def main():
    # 1. Read all IDs
    with open(local_list_file, 'r') as f:
        json_names = [line.strip() for line in f if line.strip()]

    # 2. Fire up 10 threads
    results = []
    with ThreadPoolExecutor(max_workers=10) as exe:
        future_to_name = {exe.submit(upload_file, name): name for name in json_names}
        for fut in as_completed(future_to_name):
            res = fut.result()
            # Either a string (skipped) or tuple
            if isinstance(res, tuple):
                _, success, msg = res
                print(msg)
                results.append(res)
            else:
                print(res)

    # 3. Rewrite list file removing those that succeeded
    succeeded = {name for name, ok, _ in results if ok}
    remaining = [n for n in json_names if n not in succeeded]
    with open(local_list_file, 'w') as f:
        f.write("\n".join(remaining))

if __name__ == "__main__":
    # First upload the list file itself
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(HOST, username=USER, password=PASS)
    sftp = ssh.open_sftp()
    sftp.put(local_list_file, os.path.join(remote_dir, 'hotelbeds_hotel.txt'))
    print(f"✅ hotel list uploaded to remote.")
    sftp.close()
    ssh.close()

    # Then do the concurrent JSON uploads
    main()
