
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import requests
import json
import dateutil.parser
import time

URL_API_PARIS = "https://opendata.paris.fr/api/records/1.0/search/?dataset=velib-disponibilite-en-temps-reel&q=&rows=-1&facet=name&facet=is_installed&facet=is_renting&facet=is_returning&facet=nom_arrondissement_communes"
URL_API_LILLE = "https://opendata.lillemetropole.fr/api/records/1.0/search/?dataset=vlille-realtime&q=&rows=-1&facet=libelle&facet=nom&facet=commune&facet=etat&facet=type&facet=etatconnexion"

# client = MongoClient("mongodb+srv://user:password@yyyyyyyyyy.xxxxxxxxx.mongodb.net/?retryWrites=true&w=majority", server_api=ServerApi('1'))

# db = client.vls

def get_velo(url):
    response = requests.request("GET", url)
    response_json = json.loads(response.text.encode('utf8'))
    return response_json.get("records", [])

velo_lille = get_velo(URL_API_LILLE)
velo_paris = get_velo(URL_API_PARIS)

def get_json_file():
    json_velo_lille = json.dumps(velo_lille)
    json_velo_paris = json.dumps(velo_paris)

    with open('velo_lille.json', 'w') as outfile:
        outfile.write(json_velo_lille)

    with open('velo_paris.json', 'w') as outfile:
        outfile.write(json_velo_paris)

get_json_file()

# vlilles_to_insert = [
#     {
#         '_id': elem.get('fields', {}).get('libelle'),
#         'name': elem.get('fields', {}).get('nom', '').title(),
#         'geometry': elem.get('geometry'),
#         'size': elem.get('fields', {}).get('nbvelosdispo') + elem.get('fields', {}).get('nbplacesdispo'),
#         'source': {
#             'dataset': 'Lille',
#             'id_ext': elem.get('fields', {}).get('libelle')
#         },
#         'tpe': elem.get('fields', {}).get('type', '') == 'AVEC TPE'
#     }
#     for elem in velo_lille
# ]

# try: 
#     db.stations.insert_many(vlilles_to_insert, ordered=False)
# except:
#     pass



# while True:
#     print('update')
#     vlilles = get_velo(URL_API_LILLE)
#     datas = [
#         {
#             "bike_availbale": elem.get('fields', {}).get('nbvelosdispo'),
#             "stand_availbale": elem.get('fields', {}).get('nbplacesdispo'),
#             "date": dateutil.parser.parse(elem.get('fields', {}).get('datemiseajour')),
#             "station_id": elem.get('fields', {}).get('libelle')
#         }
#         for elem in vlilles
#     ]
    
#     for data in datas:
#         db.datas.update_one({'date': data["date"], "station_id": data["station_id"]}, { "$set": data }, upsert=True)

#     time.sleep(10)