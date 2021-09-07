
from datetime import datetime
import jwt
import requests
import json

secret = '''-----BEGIN PRIVATE KEY-----
            MIGTAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBHkwdwIBAQQgR5nN4NSkhoqkwp8N
            0esMDokv+AE5QlITaE4RYYll7aOgCgYIKoZIzj0DAQehRANCAAR3ku4wPQbaHpPM
            L7hgP6btB4eR0Bj7HGHNHLQtkxEZlyCKiBuVgBYwwX+/Y4B/JogGXwYgkf78HKDx
            Z4VIPgwp
-----END PRIVATE KEY-----'''

key_id  = 'L4VGNAP2J9' # <-- your key id here
team_id = 'Z93492J9K8' # <-- your team id here
alg     = 'ES256'


token = jwt.encode(payload, secret, algorithm=alg, headers=headers)
token_str = token.decode('utf-8') 

url = "https://api.music.apple.com/v1/catalog/us/songs/203709340"

request_obj = requests.get(url, headers={'Authorization': "Bearer " + token_str})
json_dict = request_obj.json()