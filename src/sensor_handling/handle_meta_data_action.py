"""osnapi.py: This module is a wrapper for opensense.network's API"""

__author__ = "Florian Peters https://github.com/flpeters"

from contextlib import contextmanager
from typing import List

try:
    import osnapi as api
except:
    import deployment_tmp.osnapi as api

try:
    import secretmanager
except:
    import deployment_tmp.secret_manager as secretmanager
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import ConnectionFailure

mongo_db_url = secretmanager.__MONGOURL__


########## HELPERS ############
def clean_str(line: str) -> List[str]: return ''.join(line.split()).split(';')


def to_osn_date(date: str) -> str: return f'{date[:4]}-{date[4:6]}-{date[-2:]}'


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


####### PYMONGO ########
def mongo_available_check(client: MongoClient):
    try:
        client.admin.command('ismaster')
    except ConnectionFailure as e:
        print('MongoDB Server not available.')
        raise e


@contextmanager
def mongo_conn(db_url: str, check_available: bool = True, check_syntax=False) -> Collection:
    client = MongoClient(db_url,
                         appname='dwd_agent',  # displayed in mongodb server logs
                         retryWrites=True,  # retry once after network failure
                         )
    try:
        db = client['opensense']
        collection = db["vals"]
        if check_available: mongo_available_check(client)
        if check_syntax: pass  # mongo_syntax_check(collection)
        yield collection
    except Exception as e:
        raise e
    finally:
        client.close()


######### META ##########

def osn_push_sensors(sensor):
    for x in range(2, 5):
        try:
            api.login(username=secretmanager.__OSNUSERNAME__, password=secretmanager.__OSNPASSWORD__)
            return api.addSensor(sensor)['id']
        except:
            print("Failed to add sensor -> retrying {}(nd/th) time".format(x))
    raise Exception("couldnt communicate with api")


def osn_measurands(measurand):
    for x in range(2, 5):
        try:
            return api.getMeasurands(name=measurand)[0]['id']
        except:
            print("Failed to get measurands -> retrying {}(nd/th) time".format(x))
    raise Exception("couldnt communicate with api")


def osn_unitId(unitString, measurandId):
    for x in range(2, 5):
        try:
            return api.getUnits(name=unitString, measurandId=measurandId)[0]['id']
        except:
            print("Failed to get unitStrings -> retrying {}(nd/th) time".format(x))
    raise Exception("couldnt communicate with api")


def osn_licenseId():
    for x in range(2, 5):
        try:
            return api.getLicenses(shortName='DE-GeoNutzV-1.0')[0]['id']
        except:
            print("Failed to get licenseIds -> retrying {}(nd/th) time".format(x))
    raise Exception("couldnt communicate with api")


def createLocalAndRemoteSensor(dwd_id: str, measurand: str,
                               fromDate: str, toDate: str,
                               latitude: float, longitude: float) -> None:
    with mongo_conn(mongo_db_url) as collection:
        local_id = f'{dwd_id}-{measurand}'
        sensor_kind_exists = False
        sensor_exists = False
        nr_of_existing_sensors = 0

        ### SEARCH FOR EXISTING SENSOR
        mapping = collection.find_one({'local_id': local_id})
        if mapping is not None:
            sensors = mapping['sensors']
            nr_of_existing_sensors = len(sensors)
            for sensor in sensors:
                if ((sensor['earliest_day'] <= fromDate) and
                        ((sensor['latest_day'] > fromDate) or (sensor['latest_day'] == ''))):
                    ### A VALID SENSOR EXISTS AND NO NEW ONE HAS TO BE CREATED
                    if toDate != '' and sensor['latest_day'] == '':
                        pass
                        # collection.update_one({'local_id': local_id, 'sensors.idx': sensor['idx']})
                    sensor_kind_exists = True
                    sensor_exists = True
                    break
            else:
                ### NO SENSOR IN THE RIGHT RANGE EXISTS BUT EARLIER ONES DO EXISTS
                sensor_kind_exists = True
                sensor_exists = False
        else:
            ### NO SENSOR OF THIS KIND HAS BEEN ADDED BEFORE
            sensor_kind_exists = False
            sensor_exists = False

        if not sensor_exists:
            unitString = {'temperature': 'celsius',
                          'humidity': 'percent',
                          'cloudiness': 'level',
                          'air_pressure': 'hPa',
                          'wind_speed': 'm/s',
                          'wind_direction': 'degrees'}.get(measurand, None)

            if unitString is None: raise Exception(f'Station {dwd_id} has no legit unit: {measurand} -> {unitString}')

            measurandId = osn_measurands(measurand)
            unitId = osn_unitId(unitString, measurandId)
            licenseId = osn_licenseId()

            sensor = make_sensor(measurandId=measurandId, unitId=unitId, licenseId=licenseId,
                                 latitude=latitude, longitude=longitude,
                                 altitudeAboveGround=2, directionVertical=0, directionHorizontal=2,
                                 accuracy=10, sensorModel='DWD station',
                                 attributionText='Deutscher Wetterdienst (DWD)',
                                 attributionURL='ftp://ftp-cdc.dwd.de/pub/CDC/')

            osn_id = osn_push_sensors(sensor)

            if not osn_id:
                raise Exception(f'Station {dwd_id} failed to create new sensor')
            else:
                print(f'Added Sensor with dwd_id: {dwd_id} -> osn_id: {osn_id}')

            mongo_sensor = {'local_id': local_id,
                            'osn_id': osn_id,
                            'measurand': measurand,
                            'unit': unitString,
                            'idx': nr_of_existing_sensors,
                            'earliest_day': fromDate, 'latest_day': toDate,
                            'latest_sent_value': '', 'earliest_sent_value': ''}

            if not sensor_kind_exists:
                resp = collection.insert_one(document={'local_id': local_id,
                                                       'sensors': [mongo_sensor]})
            else:
                resp = collection.update_one(filter={'local_id': local_id},
                                             update={'$addToSet': {'sensors': mongo_sensor}})

            assert resp.acknowledged, f'Failed to insert new Sensor into MongoDB (dwd_id: {dwd_id}, osn_id: {osn_id})'
        else:
            print('Sensor already exists')

    if measurand == "temperature":
        createLocalAndRemoteSensor(dwd_id, "humidity",
                                   fromDate, toDate,
                                   latitude, longitude)
    if measurand == "wind_speed":
        createLocalAndRemoteSensor(dwd_id, "wind_direction",
                                   fromDate, toDate,
                                   latitude, longitude)


def main(args):
    filename = args.get("filename")
    rest_names = args.get("restfilenames")
    content = args.get("metadata")
    measurand = args.get("measurand", "temperature")
    try:
        for line in content.split("\\n"):
            line = clean_str(line)
            if len(line) < 7 or not line[0].isdigit(): continue
            stationID, heightAboveNN, latitude, longitude, fromDate, toDate, *_ = line
            fromDate = to_osn_date(fromDate)
            if toDate: toDate = to_osn_date(toDate)
            createLocalAndRemoteSensor(stationID, measurand, fromDate, toDate, float(latitude), float(longitude))
        result = {"message": "finished given metadata",
                  "filename": filename,
                  "restfilenames": rest_names}
        return result
    except Exception as e:
        secretmanager.complete_sequence(rest_names)
        result = {"error": "failed metadata because of unkown error - jump to next file"}
        print(result, e)
        return result
