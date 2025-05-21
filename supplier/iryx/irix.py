import requests

url = "https://satgurudmc.com/reseller/api/mapping/v1/archive/download/16?token=0eefc52ed99d7b1dcec296d7ea226f58413cf1d7"
output_file = "downloaded_file.zip"

response = requests.get(url)
with open(output_file, "wb") as f:
    f.write(response.content)

print("Download complete!")
