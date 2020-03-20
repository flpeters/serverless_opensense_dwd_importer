"""osnapi.py: This module is a wrapper for opensense.network's API"""

__author__ = "Florian Peters https://github.com/flpeters"

from contextlib import contextmanager
from typing import List
from datetime import datetime

try: import osnapi as api
except: import deployment_tmp.osnapi as api

try: import secretmanager
except: import deployment_tmp.secret_manager as secretmanager
    
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import ConnectionFailure

mongo_db_url = secretmanager.__MONGOURL__


################## General Helpers ##################
def clean_str(line:str) -> List[str]: return ''.join(line.split()).split(';')

def to_iso_date(timestamp:str, format:str) -> str: return datetime.strptime(timestamp, format).isoformat()


################## Opensense ################## 
def make_sensor(measurandId, unitId, licenseId,
                latitude, longitude,
                altitudeAboveGround,
                directionVertical, directionHorizontal,
                accuracy, sensorModel,
                attributionText, attributionURL):
    return {'unitId': unitId,
            'licenseId': licenseId,
            'measurandId': measurandId,
            'altitudeAboveGround': altitudeAboveGround,
            'location': {'lat': latitude, 'lng': longitude},
            'directionVertical': directionVertical,
            'directionHorizontal': directionHorizontal,
            'accuracy': accuracy,
            'sensorModel': sensorModel,
            'attributionText': attributionText,
            'attributionURL': attributionURL}

# TODO(florian): switch range to osnapi retry decorator
def osn_measurands(measurand):
    for x in range(2, 5):
        try: return api.getMeasurands(name=measurand)[0]['id']
        except: print(f'Failed to get measurands -> retrying {x}(nd/th) time')
    raise Exception('couldnt communicate with api')

def osn_unitId(unitString, measurandId):
    for x in range(2, 5):
        try: return api.getUnits(name=unitString, measurandId=measurandId)[0]['id']
        except: print(f'Failed to get measurands -> retrying {x}(nd/th) time')
    raise Exception('couldnt communicate with api')

def osn_licenseId():
    for x in range(2, 5):
        try: return api.getLicenses(shortName='DE-GeoNutzV-1.0')[0]['id']
        except: print(f'Failed to get measurands -> retrying {x}(nd/th) time')
    raise Exception('couldnt communicate with api')
    
def osn_push_sensors(sensor):
    for x in range(2, 5):
        try:
            api.login(username=secretmanager.__OSNUSERNAME__, password=secretmanager.__OSNPASSWORD__)
            return api.addSensor(sensor)['id']
        except: print(f'Failed to get measurands -> retrying {x}(nd/th) time')
    raise Exception('couldnt communicate with api')


################## PyMongo ##################
def mongo_available_check(client: MongoClient):
    try: client.admin.command('ismaster')
    except ConnectionFailure as e:
        print('MongoDB Server not available.')
        raise e

@contextmanager
def mongo_conn(db_url:str, check_available:bool=True) -> Collection:
    client = MongoClient(db_url,
                         socketTimeoutMS=10000, # default no limit
                         connectTimeoutMS=10000, # default 20 sec
                         serverSelectionTimeoutMS=10000, # default 30 sec
                         heartbeatFrequencyMS=10000, # default 10 sec
                         appname='dwd_agent', # displayed in mongodb server logs
                         retryWrites=True, # retry once after network failure
                         uuidRepresentation='standard', # default 'pythonLegacy'
                        )
    try:
        db = client['opensense']
        collection = db["vals"]
        if check_available: mongo_available_check(client)
        yield collection
    except Exception as e: raise e
    finally: client.close()


##################### MAIN #######################
def createLocalAndRemoteSensor(dwd_id:str, measurand:str,
                               fromDate:str, toDate:str,
                               latitude:float, longitude:float) -> None:
    with mongo_conn(mongo_db_url) as collection:
        local_id = f'{dwd_id}-{measurand}'
        sensor_kind_exists = False
        sensor_exists = False
        next_idx = 0

        # search for existing sensor
        mapping = collection.find_one({'local_id': local_id})
        if mapping is not None:
            sensors = mapping['sensors']
            next_idx = len(sensors)
            sensor_idxs  = [s['idx'] for s in sensors]
            while next_idx in sensor_idxs: next_idx += 1
            for sensor in sensors:
                if ((sensor['earliest_day'] <= fromDate) and
                   ((sensor['latest_day']   >  fromDate) or (sensor['latest_day'] == ''))):
                    # a valid sensor exists and no new one has to be created
                    if toDate != '' and sensor['latest_day'] == '':
                        collection.update_one(filter={'local_id' : local_id, 'sensors.idx' : sensor['idx']},
                                              update={'$set': {f'sensors.$.latest_day': toDate}})
                    sensor_kind_exists = True
                    sensor_exists = True
                    break
            else: # no sensor in the right range exists but earlier ones do exists
                sensor_kind_exists = True
                sensor_exists = False
        else: # no sensor of this kind has been added before
            sensor_kind_exists = False
            sensor_exists = False

        if not sensor_exists:
            unitString = {'temperature'   : 'celsius', 'humidity'      : 'percent',
                          'cloudiness'    : 'level', 'air_pressure'  : 'hPa',
                          'wind_speed'    : 'm/s', 'wind_direction': 'degrees'}.get(measurand, None)

            if unitString is None: raise Exception(f'Station {dwd_id} has no legit unit: {measurand} -> {unitString}')

            measurandId = osn_measurands(measurand)
            unitId = osn_unitId(unitString, measurandId)
            licenseId = osn_licenseId()

            osn_sensor = make_sensor(measurandId=measurandId, unitId=unitId, licenseId=licenseId,
                                     latitude=latitude, longitude=longitude,
                                     altitudeAboveGround=2, directionVertical=0, directionHorizontal=2,
                                     accuracy=10, sensorModel='DWD station',
                                     attributionText='Deutscher Wetterdienst (DWD)',
                                     attributionURL='ftp://ftp-cdc.dwd.de/pub/CDC/')

            osn_id = osn_push_sensors(osn_sensor)

            if not osn_id: raise Exception(f'Station {dwd_id} failed to create new sensor')
            else:          print(f'Added Sensor with dwd_id: {dwd_id} -> osn_id: {osn_id}')

            mongo_sensor = {'local_id': local_id, 'osn_id': osn_id,
                            'measurand': measurand, 'unit': unitString,
                            'idx' : next_idx,
                            'earliest_day': fromDate, 'latest_day': toDate,
                            'sent_values': []}

            if not sensor_kind_exists:
                resp = collection.insert_one(document={'local_id': local_id, 'sensors' : [mongo_sensor]})
            else:
                resp = collection.update_one(filter={'local_id' : local_id},
                                             update={'$addToSet': {'sensors': mongo_sensor}})
            assert resp.acknowledged, f'Failed to insert new Sensor into MongoDB \
                                        (dwd_id: {dwd_id}, osn_id: {osn_id})'
        else: print('Sensor already exists')

    if measurand == "temperature":
        createLocalAndRemoteSensor(dwd_id, "humidity",
                                   fromDate, toDate,
                                   latitude, longitude)
    if measurand == "wind_speed":
        createLocalAndRemoteSensor(dwd_id, "wind_direction",
                                   fromDate, toDate,
                                   latitude, longitude)

def parse_metadata(content:str, measurand:str):
    date_format = '%Y%m%d'
    for line in content.splitlines():
        line = clean_str(line)
        if len(line) < 7 or not line[0].isdigit(): continue
        stationID, heightAboveNN, latitude, longitude, fromDate, toDate, *_ = line
        fromDate = to_iso_date(timestamp=fromDate, format=date_format)
        if toDate: toDate = to_iso_date(timestamp=toDate, format=date_format)
        createLocalAndRemoteSensor(stationID, measurand, fromDate, toDate, float(latitude), float(longitude))
        
        
##################### OpenWhisk Entrypoint #######################
def main(args):
    filename = args.get('filename')
    rest_names = args.get('restfilenames')
    content = args.get('metadata')
    measurand = args.get('measurand', 'temperature')
    try:
        parse_metadata(content, measurand)
        return {'message': 'finished given metadata',
                'filename': filename, 'restfilenames': rest_names}
    except Exception as e:
        secretmanager.complete_sequence(rest_names)
        result = {'error': 'failed metadata because of unkown error - jump to next file'}
        print(result, e)
        return result
