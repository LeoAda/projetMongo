from datetime import datetime

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import requests
import json
import dateutil.parser
import time
from datetime import datetime

import os
from dotenv import load_dotenv

load_dotenv()
MONGO_USERNAME = os.getenv('MONGO_USERNAME')
MONGO_PASSWORD = os.getenv('MONGO_PASSWORD')
MONGO_CLUSTER_NAME = os.getenv('MONGO_CLUSTER_NAME')
MONGO_DATABASE_NAME = os.getenv('MONGO_DATABASE_NAME')

client = MongoClient(f"mongodb+srv://{MONGO_USERNAME}:{MONGO_PASSWORD}@{MONGO_CLUSTER_NAME}.{MONGO_DATABASE_NAME}.mongodb.net/?retryWrites=true&w=majority", server_api=ServerApi('1'))

db = client.vls

URL_API_PARIS = "https://opendata.paris.fr/api/records/1.0/search/?dataset=velib-disponibilite-en-temps-reel&q=&rows=-1&facet=name&facet=is_installed&facet=is_renting&facet=is_returning&facet=nom_arrondissement_communes"
URL_API_LILLE = "https://opendata.lillemetropole.fr/api/records/1.0/search/?dataset=vlille-realtime&q=&rows=-1&facet=libelle&facet=nom&facet=commune&facet=etat&facet=type&facet=etatconnexion"
URL_API_LYON = "https://download.data.grandlyon.com/ws/rdata/jcd_jcdecaux.jcdvelov/all.json?maxfeatures=-1&start=1"
URL_API_RENNES = "https://data.rennesmetropole.fr/api/records/1.0/search/?dataset=etat-des-stations-le-velo-star-en-temps-reel&q=&rows=-1&facet=nom&facet=etat&facet=nombreemplacementsactuels&facet=nombreemplacementsdisponibles&facet=nombrevelosdisponibles"

# Get self-services Bicycle Stations (geolocations, size, name, tpe, available): Lille, Lyon, Paris and Rennes

def get_velo(url):
    response = requests.request("GET", url)
    response_json = json.loads(response.text.encode('utf8'))
    return response_json.get("values" if url == URL_API_LYON else "records", [])

velo_lille = get_velo(URL_API_LILLE)
velo_paris = get_velo(URL_API_PARIS)
velo_lyon = get_velo(URL_API_LYON)
velo_rennes = get_velo(URL_API_RENNES)

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

# ==================== PARIS ====================

velo_paris_to_insert = [
    {
        '_id': int(elem.get('fields', {}).get('stationcode').replace('_relais', "0")),
        'name': elem.get('fields', {}).get('name', '').title(),
        'geometry': elem.get('geometry'),
        'size': elem.get('fields', {}).get('capacity'),
        'source': {
            'dataset': 'Paris',
            'id_ext': int(elem.get('fields', {}).get('stationcode').replace('_relais', "0"))
        },
        'tpe': elem.get('fields', {}).get('is_renting') == 'OUI',
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
            'coordinates': [float(elem.get('lng')), float(elem.get('lat'))]
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

# ==================== RENNES ====================

velo_rennes_to_insert = [
    {
        '_id': int(elem.get('fields', {}).get('idstation')),
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

try:
    db.stations.insert_many(velo_paris_to_insert + velo_lyon_to_insert + velo_lille_to_insert + velo_rennes_to_insert, ordered=False)
except:
    pass

# Worker who refresh and store live data for a city (history data)

def refresh_worker():
    velo_lille = get_velo(URL_API_LILLE)
    velo_paris = get_velo(URL_API_PARIS)
    velo_lyon = get_velo(URL_API_LYON)
    velo_rennes = get_velo(URL_API_RENNES)
    datas_velo_lille_update = [
        {
            "bike_available": elem.get('fields', {}).get('nbvelosdispo'),
            "stand_available": elem.get('fields', {}).get('nbplacesdispo'),
            "date": dateutil.parser.parse(elem.get('fields', {}).get('datemiseajour')),
            "station_id": elem.get('fields', {}).get('libelle'),
            'status': elem.get('fields', {}).get('etat') == 'EN SERVICE'
        }
        for elem in velo_lille
    ]
    datas_velo_paris_update = [
        {
            "bike_available": elem.get('fields', {}).get('numbikesavailable'),
            "stand_available": elem.get('fields', {}).get('numdocksavailable'),
            "date": dateutil.parser.parse(elem.get('fields', {}).get('duedate')),
            "station_id": int(elem.get('fields', {}).get('stationcode').replace('_relais', "0")),
            'status': not ((elem.get('fields', {}).get('numbikesavailable') + elem.get('fields', {}).get('numdocksavailable')) == 0 
                or elem.get('fields', {}).get('is_installed') == "NON")
        }
        for elem in velo_paris
    ]
    datas_velo_lyon_update = [
        {
            "bike_available": elem.get('available_bikes'),
            "stand_available": elem.get('available_bike_stands'),
            "date": dateutil.parser.parse(elem.get('last_update').replace('T', '') + ("+00:00")),
            "station_id": elem.get('number'),
            'status': elem.get('status') == 'OPEN'
        }
        for elem in velo_lyon
    ]
    datas_velo_rennes_update = [
        {
            "bike_available": elem.get('fields', {}).get('nombrevelosdisponibles'),
            "stand_available": elem.get('fields', {}).get('nombreemplacementsdisponibles'),
            "date": dateutil.parser.parse(elem.get('fields', {}).get('lastupdate')),
            "station_id": int(elem.get('fields', {}).get('idstation')),
            'status': elem.get('fields', {}).get('etat') == 'En fonctionnement'
        }
        for elem in velo_rennes
    ]

    datas = datas_velo_lille_update + datas_velo_paris_update + datas_velo_lyon_update + datas_velo_rennes_update

    for data in datas:
        db.datas.update_one({'date': data["date"], "station_id": data["station_id"]}, {"$set": data}, upsert=True)

    #Index needed for geoNear and text search
    db.stations.create_index([('geometry', '2dsphere')])
    db.stations.create_index([('name', 'text')])

# ================================
# *------- Client program -------*
# ================================

#Return the closest station from a position
def get_nearest_stations(lat, lng, nb_stations=1):
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
    return stations[:nb_stations]

#Return all stations available from a list of stations
def get_available_stations(stations):
    available_stations = []
    for station in stations:
        if db.datas.find({'station_id': station['_id']}).sort('date', -1).limit(1)[0]['status']:
            available_stations.append(station)
    return available_stations

def get_nearest_available_station(lat, lng, nb_stations=1):
    stations = get_nearest_stations(lat, lng, nb_stations)
    return get_available_stations(stations)

# ================================
# *----- Business program -------*
# ================================

#Return sorted list of stations by score of similarity from a name
def get_stations_by_name(name):
    stations = db.stations.find({
        "$text": {"$search": name}
    }, {
        "score": {"$meta": "textScore"}
    }).sort("score")
    return list(stations)

def update_stations_name(name, new_name):
    db.stations.update_one({'name': name}, { "$set": { 'name': new_name } })

def update_stations_size(name, new_size):
    db.stations.update_one({'name': name}, { "$set": { 'size': new_size } })

def update_stations_tpe(name, new_tpe):
    db.stations.update_one({'name': name}, { "$set": { 'tpe': new_tpe } })

def delete_stations(name):
    db.stations.delete_one({'name': name})

#Return function around a position
def get_stations_around(lat, lng, radius):
    stations = db.stations.find({
        'geometry': {
            '$geoWithin': {
                '$centerSphere': [[lng, lat], radius / 6378.1]
            }
        }
    })
    return list(stations)

def deactivate_stations(stations):
    deactivate_stations = []
    for station in stations:
        if len(get_available_stations([station])) == 1:
            db.datas.insert_one({
                "bike_available": 0,
                "stand_available": 0,
                "date": datetime.now(),
                "station_id": station['_id'],
                "status": False
            })
            deactivate_stations.append(station)
    return deactivate_stations

def desactivate_stations_around(lat, lng, radius):
    stations = get_stations_around(lat, lng, radius)
    return deactivate_stations(stations)

#return all datas collection
def get_available():
    stations = list(db.datas.find())
    #filter station to get only station with a date within 18 and 19hours
    stations = [station for station in stations if station['date'].hour in [18, 19]]
    #filter to keep only the most recent date
    stations = [max([station for station in stations if station['station_id'] == station_id], key=lambda x: x['date']) for station_id in set([station['station_id'] for station in stations])]
    #filter to keep only station with at least 1 bike_available
    stations = [station for station in stations if station['bike_available'] > 0]
    #filter to keep only station with at least 1 stand_available
    stations = [station for station in stations if station['stand_available'] > 0]
    #filter to keep only station with a ratio of available bikes to available stands < 0.2
    stations = [station for station in stations if station['bike_available'] / station['stand_available'] < 0.2]
    return stations

while True:
    refresh_worker()

    print("5 stations valides les plus proches de Jb Lebas")
    print(get_nearest_available_station(50.626457, 3.068455, 5))

    print("Recherche d'une station avec comme mot clé 'Quai'")
    print(get_stations_by_name("quai"))

    print("Mise à jour du nom de la station 'Charonne - Robert Et Sonia Delauney' en 'Charonne'")
    update_stations_name("Charonne - Robert Et Sonia Delauney","Charonne")

    print("Mise à jour de la taille de la station 'Charonne' a 3")
    update_stations_size("Charonne",3)

    print("Mise à jour de la présence de tpe de la station 'Charonne' a False")
    update_stations_tpe("Charonne", False)

    print("Suppression de la station 'Mairie Du 12Ème'")
    delete_stations("Mairie Du 12Ème")

    print("Désactivation des stations dans un rayon de 5km autour du point 50.626457, 3.068455 représentant la station 'Jb Lebas'")
    desactivate_stations_around(50.626457, 3.068455, 5)

    print("Toutes les stations avec un ratio de vélos disponibles sur places disponibles < 0.2 de 18h à 19h du lundi au vendredi")
    for station in get_available():
        print(station)
        
    time.sleep(10)
