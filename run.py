
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import requests
import json
import dateutil.parser
import time


import os
from dotenv import load_dotenv

from geopy.distance import distance

# Import dotenv variable
load_dotenv()
MONGO_USERNAME = os.getenv('MONGO_USERNAME')
MONGO_PASSWORD = os.getenv('MONGO_PASSWORD')
MONGO_CLUSTER_NAME = os.getenv('MONGO_CLUSTER_NAME')
MONGO_DATABASE_NAME = os.getenv('MONGO_DATABASE_NAME')


client = MongoClient(f"mongodb+srv://{MONGO_USERNAME}:{MONGO_PASSWORD}@{MONGO_CLUSTER_NAME}.{MONGO_DATABASE_NAME}.mongodb.net/?retryWrites=true&w=majority", server_api=ServerApi('1'))

db = client.vls

def get_vlille():
    url = "https://opendata.lillemetropole.fr/api/records/1.0/search/?dataset=vlille-realtime&q=&rows=3000&facet=libelle&facet=nom&facet=commune&facet=etat&facet=type&facet=etatconnexion"
    response = requests.request("GET", url)
    response_json = json.loads(response.text.encode('utf8'))
    return response_json.get("records", [])


vlilles = get_vlille()

vlilles_to_insert = [
    {
        '_id': elem.get('fields', {}).get('libelle'),
        'name': elem.get('fields', {}).get('nom', '').title(),
        'geometry': elem.get('geometry'),
        'size': elem.get('fields', {}).get('nbvelosdispo') + elem.get('fields', {}).get('nbplacesdispo'),
        'source': {
            'dataset': 'Lille',
            'id_ext': elem.get('fields', {}).get('libelle')
        },
        'tpe': elem.get('fields', {}).get('type', '') == 'AVEC TPE'
    }
    for elem in vlilles
]

try:
    db.stations.insert_many(vlilles_to_insert, ordered=False)
except:
    pass

point_a = (41.490080000000000000, -71.312796)
point_b = (41.4, -81.695391)
print(distance(point_a, point_b).km)

def get_nearest_station(lat, lng):
    stations = db.stations.find({
        'geometry': {
            '$near': {
                '$geometry': {
                    'type': 'Point',
                    'coordinates': [lng, lat]
                }
            }
        }
    })
    return stations[0]

while True:
    print('update')
    vlilles = get_vlille()
    datas = [
        {
            "bike_availbale": elem.get('fields', {}).get('nbvelosdispo'),
            "stand_availbale": elem.get('fields', {}).get('nbplacesdispo'),
            "date": dateutil.parser.parse(elem.get('fields', {}).get('datemiseajour')),
            "station_id": elem.get('fields', {}).get('libelle')
        }
        for elem in vlilles
    ]

    for data in datas:
        db.datas.update_one({'date': data["date"], "station_id": data["station_id"]}, { "$set": data }, upsert=True)

    #put 2d object in db
    db.stations.create_index([('geometry', '2dsphere')])
    
    print(get_nearest_station(50.626457, 3.068455))
    time.sleep(10)