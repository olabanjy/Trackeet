from datetime import datetime
import jwt
import requests
import json

secret_key = '''
-----BEGIN PRIVATE KEY-----
MIGTAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBHkwdwIBAQQgvR2+UVWhxDzT0FJf
lFZNnbNi5n42cItWbi+4nws1u3mgCgYIKoZIzj0DAQehRANCAATY0feUFHh0OYos
VEWSw18t4KOvYOQjOECkTmnAgTyBiR+0aZBFJy5xl3g2PBRIoNimmIgd4u6vF9y/
oqYkNDmb
-----END PRIVATE KEY-----
'''

key_id  = 'KTK94J58L9' # <-- your key id here
team_id = 'Z93492J9K8' # <-- your team id here
alg     = 'ES256'
iat     = int(datetime.utcnow().strftime("%S")) # set issued at to now
exp     = iat + 86400 # add e.g. 24h from now for expiration (24 * 3600secs == 86400)

payload = {
    'iss': team_id,
    'iat': iat,
    'exp': exp
}

headers = {
    'alg': alg,
    'kid': key_id
}

token = jwt.encode(payload, secret_key, algorithm=alg, headers=headers)
token_str = token.decode('utf-8')

url = "https://api.music.apple.com/v1/catalog/us/songs/203709340"
# print (f"curl -v -H 'Authorization: Bearer {token_str}' {url}")


res = requests.get(url, headers={'Authorization': "Bearer " + token_str})
# result = json.loads(res.text)
print(res.headers)