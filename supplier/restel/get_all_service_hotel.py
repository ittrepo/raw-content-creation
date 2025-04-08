import urllib.parse
import requests

# XML data
xml_data = """<?xml version="1.0" encoding="UTF-8"?>
<peticion>
  <tipo>17</tipo>
  <nombre>Servicio de listado de hoteles</nombre>
  <agencia>Agencia prueba</agencia>
</peticion>"""

# URL-encode the XML data
encoded_xml = urllib.parse.quote(xml_data)

# Construct the URL with the encoded XML
url = f"http://xml.hotelresb2b.com/xml/listen_xml.jsp?codigousu=ZVYE&clausu=xml514142&afiliacio=RS&secacc=151003&xml={encoded_xml}"

headers = {
  'Cookie': 'JSESSIONID=aaaodjlEZaLhM_vAad2xz'
}

response = requests.get(url, headers=headers)

print(response.text)
