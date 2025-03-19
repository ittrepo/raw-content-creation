import base64
credentials = "GTRSGTEST:Glo@11728139"
encoded_credentials = base64.b64encode(credentials.encode()).decode()
print(encoded_credentials)
