import requests
import json

url = "https://api-sandbox.grnconnect.com/api/v3/hotels?hcode=1158431&version=2.0"

payload = {}
files={}
headers = {
  'Content-Type': 'application/json',
  'Accept': 'application/json',
  'Accept-Encoding': 'application/gzip',
  'api-key': 'cda7a3d1a85a048030eca511a2805c59'
}

response = requests.request("GET", url, headers=headers, data=payload, files=files)

print(response.text)



here get data like

{
    "total": 1,
    "hotels": [
        {
            "acc_name": "Hotel",
            "acc_type": "0",
            "address": "595 Jiu Jiang Road Huangpu District Shanghai  Jiu Jiang Road",
            "category": "5",
            "chain_name": "",
            "city_code": "121659",
            "code": "1158431",
            "country": "CN",
            "description": "This hotel is (fees apply).",
            "dest_code": "100738",
            "facilities": "Wakeup service ;  Adjoining rooms;",
            "latitude": "31.234722014118",
            "longitude": "121.47936742328",
            "name": "Howard Johnson Plaza Shanghai",
            "postal_code": "200001",
            "recommended": "0"
        }
    ]
}


this information to get

"country": "CN",
and
"city_code": "121659",

this and 

call bellow endpoint

import requests
import json

url = "https://api-sandbox.grnconnect.com/api/v3/countries/CN"

payload = {}
headers = {
  'API-key': 'cda7a3d1a85a048030eca511a2805c59',
  'Accept': 'application/json',
  'Content-Type': 'application/json'
}

response = requests.request("GET", url, headers=headers, data=payload)

print(response.text)



and here get


{
    "country": {
        "code": "CN",
        "code3": "CHN",
        "name": "China"
    },
    "total": 1
}



import requests
import json

url = "https://api-sandbox.grnconnect.com/api/v3/cities/121659?version=2.0"

payload = {}
headers = {
  'API-key': 'cda7a3d1a85a048030eca511a2805c59',
  'Accept': 'application/json',
  'Content-Type': 'application/json'
}

response = requests.request("GET", url, headers=headers, data=payload)

print(response.text)


and get 

{
    "city": {
        "code": "121659",
        "country": "CN",
        "dest_code": "100738",
        "name": "Shanghai"
    },
    "total": 1
}

this info. 

and call another endpoint

import requests
import json

url = "https://api-sandbox.grnconnect.com//api/v3/hotels/1158431/images?version=2.0"

payload = {}
files={}
headers = {
  'Content-Type': 'application/json',
  'Accept': 'application/json',
  'Accept-Encoding': 'application/gzip',
  'api-key': 'cda7a3d1a85a048030eca511a2805c59'
}

response = requests.request("GET", url, headers=headers, data=payload, files=files)

print(response.text)


and get output is 

{
    "hotel_code": "1158431",
    "images": {
        "regular": [
            {
                "caption": "Leisure and Sport Facilities",
                "main_image": true,
                "path": "1158431/39849a74ef0efde2850da40d2d32c71c.jpg",
                "url": "https://images.grnconnect.com/1158431/39849a74ef0efde2850da40d2d32c71c.jpg"
            },
            {
                "caption": "Amenities and Services",
                "main_image": false,
                "path": "1158431/fbe2b4c380eb1a8126fbc175dd51b4ce.jpg",
                "url": "https://images.grnconnect.com/1158431/fbe2b4c380eb1a8126fbc175dd51b4ce.jpg"
            },
        ]
    }
}


now I need add all output result a json format. 

create a function like
def get_supplier_own_raw_data(hotel_id):

    return data


and save it a json file like

hotel_id.json file. file path save 

BASE_PATH = "D:/content_for_hotel_json/cdn_row_collection/grnconnect_new"

here