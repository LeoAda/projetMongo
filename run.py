from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import requests
import json
import dateutil.parser
import time

URL_API_PARIS = "https://opendata.paris.fr/api/records/1.0/search/?dataset=velib-disponibilite-en-temps-reel&q=&rows=-1&facet=name&facet=is_installed&facet=is_renting&facet=is_returning&facet=nom_arrondissement_communes"
URL_API_LILLE = "https://opendata.lillemetropole.fr/api/records/1.0/search/?dataset=vlille-realtime&q=&rows=-1&facet=libelle&facet=nom&facet=commune&facet=etat&facet=type&facet=etatconnexion"
URL_API_LYON = "https://download.data.grandlyon.com/ws/rdata/jcd_jcdecaux.jcdvelov/all.json?maxfeatures=-1&start=1"
URL_API_RENNES = "https://data.rennesmetropole.fr/api/records/1.0/search/?dataset=etat-des-stations-le-velo-star-en-temps-reel&q=&rows=-1&facet=nom&facet=etat&facet=nombreemplacementsactuels&facet=nombreemplacementsdisponibles&facet=nombrevelosdisponibles"

# client = MongoClient("mongodb+srv://user:password@yyyyyyyyyy.xxxxxxxxx.mongodb.net/?retryWrites=true&w=majority", server_api=ServerApi('1'))

# db = client.vls

def get_velo(url):
    response = requests.request("GET", url)
    response_json = json.loads(response.text.encode('utf8'))
    return response_json.get("values" if url == URL_API_LYON else "records", [])

velo_lille = get_velo(URL_API_LILLE)
velo_paris = get_velo(URL_API_PARIS)
velo_lyon = get_velo(URL_API_LYON)
velo_rennes = get_velo(URL_API_RENNES)

def get_json_file():
    json_velo_lille = json.dumps(velo_lille)
    json_velo_paris = json.dumps(velo_paris)
    json_velo_lyon = json.dumps(velo_lyon)
    json_velo_rennes = json.dumps(velo_rennes)

    with open('velo_lille.json', 'w') as outfile:
        outfile.write(json_velo_lille)

    with open('velo_paris.json', 'w') as outfile:
        outfile.write(json_velo_paris)

    with open('velo_lyon.json', 'w') as outfile:
        outfile.write(json_velo_lyon)
    
    with open('velo_rennes.json', 'w') as outfile:
        outfile.write(json_velo_rennes)

# get_json_file()

# ==================== LILLE ====================

velo_lille_to_insert = [
    {
        '_id': elem.get('fields', {}).get('libelle'),
        'name': elem.get('fields', {}).get('nom', '').title(),
        'geometry': elem.get('geometry'),
        'size': elem.get('fields', {}).get('nbvelosdispo') + elem.get('fields', {}).get('nbplacesdispo'),
        'source': {
            'dataset': 'Lille',
            'id_ext': elem.get('fields', {}).get('libelle')
        },
        'tpe': elem.get('fields', {}).get('type', '') == 'AVEC TPE',
    }
    for elem in velo_lille
]

datas_velo_lille_update = [
        {
            "bike_availbale": elem.get('fields', {}).get('nbvelosdispo'),
            "stand_availbale": elem.get('fields', {}).get('nbplacesdispo'),
            "date": dateutil.parser.parse(elem.get('fields', {}).get('datemiseajour')),
            "station_id": elem.get('fields', {}).get('libelle'),
            'status': elem.get('fields', {}).get('etat') == 'EN SERVICE'
        }
        for elem in velo_lille
    ]

# ==================== PARIS ====================

velo_paris_to_insert = [
    {
        '_id': elem.get('fields', {}).get('stationcode'),
        'name': elem.get('fields', {}).get('name', '').title(),
        'geometry': elem.get('geometry'),
        'size': elem.get('fields', {}).get('capacity'),
        'source': {
            'dataset': 'Paris',
            'id_ext': elem.get('fields', {}).get('stationcode')
        },
        'tpe': elem.get('fields', {}).get('is_renting') == 'OUI',
    }
    for elem in velo_paris
]

datas_velo_paris_update = [
        {
            "bike_availbale": elem.get('fields', {}).get('numbikesavailable'),
            "stand_availbale": elem.get('fields', {}).get('numdocksavailable'),
            "date": dateutil.parser.parse(elem.get('fields', {}).get('duedate')),
            "station_id": elem.get('fields', {}).get('stationcode'),
            'status': not ((elem.get('fields', {}).get('numbikesavailable') + elem.get('fields', {}).get('numdocksavailable')) == 0 
                or elem.get('fields', {}).get('is_installed') == "NON")
        }
        for elem in velo_paris
    ]

# ==================== LYON ====================

velo_lyon_to_insert = [
    {
        '_id': elem.get('number'),
        'name': elem.get('name', '').title(),
        'geometry': {
            'type': 'Point',
            'coordinates': [elem.get('lng'), elem.get('lat')]
        },
        'size': elem.get('bike_stands'),
        'source': {
            'dataset': 'Lyon',
            'id_ext': elem.get('number')
        },
        'tpe': elem.get('banking', ''),
    }
    for elem in velo_lyon
]

datas_velo_lyon_update = [
        {
            "bike_availbale": elem.get('available_bikes'),
            "stand_availbale": elem.get('available_bike_stands'),
            "date": dateutil.parser.parse(elem.get('last_update').replace('T', '') + ("+00:00")),
            "station_id": elem.get('number'),
            'status': elem.get('status') == 'OPEN'
        }
        for elem in velo_lyon
    ]

# ==================== RENNES ====================

velo_rennes_to_insert = [
    {
        '_id': elem.get('fields', {}).get('idstation'),
        'name': elem.get('fields', {}).get('nom', '').title(),
        'geometry': elem.get('geometry'),
        'size': elem.get('fields', {}).get('nombreemplacementsactuels'),
        'source': {
            'dataset': 'Rennes',
            'id_ext': elem.get('fields', {}).get('idstation')
        },
        'tpe': 'N/A',
    }
    for elem in velo_rennes
]

datas_velo_rennes_update = [
        {
            "bike_availbale": elem.get('fields', {}).get('nombrevelosdisponibles'),
            "stand_availbale": elem.get('fields', {}).get('nombreemplacementsdisponibles'),
            "date": dateutil.parser.parse(elem.get('fields', {}).get('lastupdate')),
            "station_id": elem.get('fields', {}).get('idstation'),
            'status': elem.get('fields', {}).get('etat') == 'En fonctionnement'
        }
        for elem in velo_rennes
    ]

print("Lille : ", velo_lille_to_insert[0])
print("Lille : ", datas_velo_lille_update[0])
print("Paris : ", velo_paris_to_insert[0])
print("Paris : ", datas_velo_paris_update[0])
print("Lyon : ", velo_lyon_to_insert[0])
print("Lyon : ", datas_velo_lyon_update[0])
print("Rennes : ", velo_rennes_to_insert[0])
print("Rennes : ", datas_velo_rennes_update[0])


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