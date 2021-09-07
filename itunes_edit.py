'''

Name:ultra
Key ID:KTK94J58L9
Services:MusicKit


Name:digger
Key ID:L4VGNAP2J9
Services:MusicKit

'''

secret_key = ''' -----BEGIN PRIVATE KEY-----
            MIGTAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBHkwdwIBAQQgR5nN4NSkhoqkwp8N
            0esMDokv+AE5QlITaE4RYYll7aOgCgYIKoZIzj0DAQehRANCAAR3ku4wPQbaHpPM
            L7hgP6btB4eR0Bj7HGHNHLQtkxEZlyCKiBuVgBYwwX+/Y4B/JogGXwYgkf78HKDx
            Z4VIPgwp
            -----END PRIVATE KEY----- '''

key_id = 'L4VGNAP2J9'
team_id = 'Z93492J9K8'


from authlib.specs.rfc7519 import jwt
import requests, json 

import applemusicpy

# req = requests.get("https://api.music.apple.com/v1/catalog/us/songs?filter[isrc]=191773665364")




# print(req.headers)
# print(req)

am = applemusicpy.AppleMusic(secret_key, key_id, team_id)
results = am.songs_by_isrc('191773665364',storefront='us')

print(result)

# for item in results['results']['albums']['data']:
#     print(item['attributes']['name'])
