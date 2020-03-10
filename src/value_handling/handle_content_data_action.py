"""osnapi.py: This module is a wrapper for opensense.network's API"""

__author__ = "Florian Peters https://github.com/flpeters"

import time
from contextlib import contextmanager
from datetime import datetime
from typing import List, Callable

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
                         socketTimeoutMS=10000,  # default no limit
                         connectTimeoutMS=10000,  # default 20 sec
                         serverSelectionTimeoutMS=10000,  # default 30 sec
                         heartbeatFrequencyMS=10000,  # default 10 sec
                         appname='dwd_agent',  # displayed in mongodb server logs
                         retryWrites=True,  # retry once after network failure
                         # compressors=[], # list for wire protocol compression negotiation - default undocumented
                         zlibCompressionLevel=-1,  # default -1 (no compression) 1 (best speed) to 9 (best compression)
                         uuidRepresentation='standard',  # default 'pythonLegacy'
                         # Write Concern options - used to modify behavior on writes
                         # Read Preferences - replica set read preferences
                         # Authentication - username and password
                         # TLS / SSL configuration - security settings for low level networking
                         # Read Concern Level - level of isolation for read operations
                         # Client side encryption (beta) - default None
                         )
    try:
        db = client['opensense']
        collection = db["vals"]
        if check_available: mongo_available_check(client)
        if check_syntax: pass  # mongo_syntax_check(collection)
        if check_syntax: pass  # mongo_syntax_check(collection)
        yield collection
    except Exception as e:
        raise e
    finally:
        client.close()


################## HELPERS ##################
def clean_str(line: str) -> List[str]: return ''.join(line.split()).split(';')


def to_float_or_none(x: str) -> float:
    try:
        return float(x)
    except ValueError as e:
        print(f'ValueError: {e}')
        return None


def to_int_or_none(x: str) -> int:
    try:
        return int(x)
    except ValueError as e:
        print(f'ValueError: {e}')
        return None


def get_indices(fieldDefs: list) -> tuple:
    def _idx_of(*args):
        for s in args:  # return first found, not last found (does this make a difference?)
            if s in fieldDefs: return fieldDefs.index(s)

    # Meta Information
    stationIDIndex = _idx_of("STATIONS_ID")
    dateIndex = _idx_of("MESS_DATUM")
    qualityIndex = _idx_of('QUALITAETS_NIVEAU', 'QN_9', 'QN_8', 'QN_7', 'QN_3')
    structure_version_index = _idx_of("STRUKTUR_VERSION")
    # Content Data
    airTemperatureIndex = _idx_of('LUFTTEMPERATUR', 'TT_TU')
    humidityIndex = _idx_of('REL_FEUCHTE', 'RF_TU')
    cloudinessIndex = _idx_of('GESAMT_BEDECKUNGSGRAD', 'V_N')  # only values between 1 and 8
    precipitationYesNoIndex = _idx_of('NIEDERSCHLAG_GEFALLEN_IND',
                                      'RS_IND')  # only values 0 or 1: 0->no prec., 1->prec.
    precipitationAmountIndex = _idx_of('NIEDERSCHLAGSHOEHE', 'R1')  # value in mm
    precipitationTypeIndex = _idx_of('NIEDERSCHLAGSFORM', 'WRTR')  # 9 is exceptionValue, valid values between 1 and 8
    airPressureNNIndex = _idx_of('LUFTDRUCK_REDUZIERT', 'P')
    airPressureIndex = _idx_of('LUFTDRUCK_STATIONSHOEHE', 'P0')
    sunshineMinsPerHourIndex = _idx_of('STUNDENSUMME_SONNENSCHEIN', 'SD_SO')
    windSpeedIndex = _idx_of('WINDGESCHWINDIGKEIT', 'F')
    windDirectionIndex = _idx_of('WINDRICHTUNG', 'D')

    return (stationIDIndex, dateIndex, qualityIndex,
            structure_version_index, airTemperatureIndex,
            humidityIndex, cloudinessIndex,
            precipitationYesNoIndex,
            precipitationAmountIndex, precipitationTypeIndex,
            airPressureNNIndex, airPressureIndex,
            sunshineMinsPerHourIndex,
            windSpeedIndex, windDirectionIndex)


def belongs_to_sensor(from_: str, ts: str, to_: str) -> bool: return (from_ <= ts and (ts <= to_ or to_ == ''))


def batchify(from_: int, to_: int, max_batch_size: int) -> List[tuple]:
    return [(i, min(to_, i + max_batch_size)) for i in range(from_, to_, max_batch_size)]


def find_transition(start: int, end: int, list_: list, condition: Callable) -> int:
    pivot = (start + end) // 2  # middle point between start and end of the list
    while end - start > 1:
        if condition(list_[pivot]):
            if not condition(list_[pivot + 1]):
                return pivot
            else:
                start, pivot = pivot, (pivot + end) // 2  # move to the right
        else:
            if condition(list_[pivot - 1]):
                return pivot
            else:
                end, pivot = pivot, (start + pivot) // 2  # move to the left


def mongo_sensors_by_local_id(dwd_id: int, measurand: str) -> list:
    with mongo_conn(mongo_db_url) as collection:
        local_id = f'{dwd_id}-{measurand}'
        mapping = collection.find_one({'local_id': local_id})
        if mapping is not None:
            sensors = mapping['sensors']
        else:
            raise Exception(f'No sensor mapping found for local id: {local_id}')
    return sorted(sensors, key=lambda x: x['earliest_day'])


def seperate_by_sensor(dates: list, sensors: list, dwd_id) -> dict:
    chunks = {}
    start = 0
    for i, sensor in enumerate(sensors):
        print(f'|---------------sensor: {i}---------------|')
        from_ = sensor['earliest_day']
        to_ = sensor['latest_day']

        # if sensors['latest_sent_value'] != '': from_ = sensors['latest_sent_value']
        # sensors['earliest_sent_value']

        ts = dates[start]
        if belongs_to_sensor(from_, dates[start], to_):
            # look for the end of the interval
            print('start position belongs to sensor')
            ts = dates[-1]
            if ts <= to_ or to_ == '':
                print('end position also belongs to sensor')
                # special case where the whole dataset belongs to the same sensor
                print('all remaining data belongs to this sensor -> end')
                chunks[i] = (start, len(dates))
                break
            else:
                print('end position does not belong to sensor -> searching for transition')
                # not every value belongs to the same sensor -> search for transition
                pivot = find_transition(start, len(dates) - 1, dates,
                                        lambda x: belongs_to_sensor(from_, x, to_))
                print('transition found')
                chunks[i] = (start, pivot)  # - 1?
                start = pivot + 1  # since it's an inclusive search, advance counter by one
                continue
            pass
        else:
            if ts < from_:
                print(
                    f'Missing Sensor detected (dwd_id: {dwd_id}):\n                value is from {ts} but sensor covers {from_} - {to_}')
                print('value is from an earlier date than this sensors interval start')
                ts = dates[-1]
                if from_ < ts:
                    print(
                        'last value is from a later date than this sensors interval start -> there must be a transition')
                    if belongs_to_sensor(from_, ts, to_):
                        print('last value belongs to this sensors interval -> search for start transition')
                        # special case where from some point to the right onwards, all values belong to this sensor
                        # search for start
                        pivot = find_transition(start, len(dates) - 1, dates,
                                                lambda x: not belongs_to_sensor(from_, x, to_))
                        print('transition found')
                        print('all remaining data belongs to this sensor -> end')
                        chunks[i] = (pivot, len(dates))  # all values from here belong to this sensor
                        break
                    else:
                        print(
                            'last value does not belong to this sensors interval -> check values util the end of interval')
                        # the might be some values to the right that still belong to this sensor.
                        # check values that are smaller than this sensors to_ to see if they're in it's interval
                        #
                        for j, ts in enumerate(dates[start:]):
                            if to_ < ts:  # reached the end of this sensors interval. let a later sensor handle this now
                                print('end of interval reached without finding valid valued -> try next sensor')
                                start = start + j
                                break
                            else:
                                if belongs_to_sensor(from_, ts, to_):
                                    print('found a valid value -> this is the start of the interval -> search for end')
                                    # great! found the start. now search for the end
                                    pivot = find_transition(start, len(dates) - 1, dates,
                                                            lambda x: belongs_to_sensor(from_, x, to_))
                                    print('transition found')
                                    chunks[i] = (start + j, pivot)
                                    start = pivot + 1  # inclusive search, advance counter by one
                                    break
                                else:
                                    continue
                        continue
                    pass
                else:
                    print(
                        'last value is still from before this sensors interval ->                     only historical data here, from a missing sensor -> ending search altogether')
                    # there is only historical data here
                    # no sensor after this one will find any datapoints here, since they're sorted
                    break
            else:
                print('start position is at a later date than this sensors interval -> let a later sensor handle this')
                # this sensor is too old and has no values
                # let a more recent sensor check this position
                continue
    return chunks


def osn_push_valuebulk(valuebulk: dict) -> bool:
    try:
        return api.addMultipleValues(body=valuebulk) == 'OK'

    except PermissionError as pe:
        print(f'Failed pushing values once, due to permission error.\n\t--> sending login and retrying second time.')
        try:
            api.login(username=secretmanager.__OSNUSERNAME__, password=secretmanager.__OSNPASSWORD__)
        except Exception as e2:
            print(f'WARNING: Failed to log in ({e2}).\n\t--> Skipping values.')

    except TimeoutError as te:
        print(f'Failed pushing values once, due to timeout error.\n\t--> retrying')
    except Exception as e:
        print(f'Failed pushing values once, due to unhandled error.\n\t--> retrying')

    for x in range(2, 5):
        try:
            return api.addMultipleValues(body=valuebulk) == 'OK'
        except Exception as e:
            print(f'WARNING: Failed pushing values a {x}(nd/th) time of maximum 5 retries({e}).\n\t--> retrying.')

    return False


##################### MAIN FUNCTION #######################


def main(args):
    csv = args.get("csv")
    rest_names = args.get("restfilenames")
    measurand = args.get("measurand", 'temperature')
    if csv is None:
        return {"error": "seuquence should be stopped"}
    try:
        logged_action = False
        lines = csv.split("\\r\\n")
        first_line = lines.pop(0)
        lines.pop(0)
        first_line = clean_str(first_line)
        len_first_line = len(first_line)
        field_defs = get_indices(first_line)
        print("fielddefs", field_defs)

        if len_first_line < 5: raise Exception(f'Nr of fields is lower than expected: {first_line}')

        dwd_id_idx, date_idx, quality_idx, structure_version_idx, \
        air_temperature_idx, humidity_idx, cloudiness_idx, \
        precipitation_yes_no_idx, precipitation_amount_idx, precipitation_type_idx, \
        air_pressure_nn_idx, air_pressure_idx, \
        sunshine_mins_per_hour_idx, wind_speed_idx, wind_direction_idx = field_defs

        if dwd_id_idx is None: raise Exception(f'File does not contain a dwd_id index: {field_defs}')
        if date_idx is None: raise Exception(f'File does not contain a Timestamp index: {field_defs}')
        if quality_idx is None: pass  # Not Implemented yet and not essential
        if structure_version_idx is None: pass  # Not Implemented yet and not essential

        print("len lines", len(lines))
        lines = [clean_str(x) for x in lines if not (x is None or x == '')]

        dwd_id = lines[0][dwd_id_idx]
        if not dwd_id.isdigit() or dwd_id is None:
            raise Exception(f'Not a valid dwd_id: {dwd_id}')
        print("dwdid", dwd_id)

        lines = sorted(lines, key=lambda x: x[date_idx])
        lines = [line for line in lines if len(line) > 5 and len(line) == len_first_line]
        print("lenlines", len(lines))

        lines = list(zip(*lines))  # transpose

        dates = []
        hours = []
        iso_dates = []
        for ts in lines[date_idx]:
            year, month, day, hour = ts[:4], ts[4:6], ts[6:8], ts[-2:]
            dates.append(f'{year}-{month}-{day}')
            hours.append(f'{year}-{month}-{day}-{hour}')
            iso_dates.append(datetime(int(year), int(month), int(day), int(hour)).isoformat())  # replace with strptime?

        print('-' * 50)

        valuebulk = {'collapsedMessages': []}
        messages = valuebulk['collapsedMessages']

        def _add_float_values(_idx: int, _i: int, _j: int, _osn_id: int):
            for _iso_date, _value in zip(iso_dates[_i:_j],
                                         lines[_idx][_i:_j]):
                _value = to_float_or_none(_value)
                if _value is not None and _value != -999.0:
                    messages.append({'sensorId': _osn_id,
                                     'timestamp': _iso_date,
                                     'numberValue': _value})

        def _add_cloudiness_values(_idx: int, _i: int, _j: int, _osn_id: int):
            for _iso_date, _value in zip(iso_dates[_i:_j],
                                         lines[_idx][_i:_j]):
                _value = to_int_or_none(_value)
                if _value is not None and 0 < _value < 8:
                    _value *= 1 / 8  # map to float between 0 and 1
                    messages.append({'sensorId': _osn_id,
                                     'timestamp': _iso_date,
                                     'numberValue': _value})

        def _process_chunks(_idx: int, _chunks: tuple, _sensors: dict, _measurand: str, _add_values: Callable):
            nonlocal valuebulk, messages, logged_action
            print("chunks ", _chunks)
            _local_id = f'{dwd_id}-{_measurand}'
            with mongo_conn(mongo_db_url) as collection:
                if not logged_action:
                    collection.update({"_id": 5}, {"$inc": {"actionCount": 1}})
                    logged_action = True
                for _sensor_id in _chunks:
                    _sensor = _sensors[_sensor_id]
                    _osn_id = _sensor['osn_id']
                    _sensor_idx = _sensor['idx']
                    _chunk = _chunks[_sensor_id]
                    if _sensor['earliest_sent_value'] == '':
                        collection.update_one(filter={'local_id': _local_id},
                                              update={'$set':
                                                          {f'sensors.{_sensor_idx}.earliest_sent_value': dates[_chunk[0]]}})
                    for i, j in batchify(*_chunk, max_batch_size=3000):
                        _add_values(_idx, i, j, _osn_id)
                        t0 = time.time()
                        collection.update({"_id": 2}, {"$inc": {"aimedValueCount": len(messages)}})
                        if osn_push_valuebulk(valuebulk):
                            print(f'Pushed {len(messages)} values to OSN. took: {round(time.time() - t0, 5)} sec')
                            collection.update_one(filter={'local_id': _local_id},
                                                  update={
                                                      '$set': {f'sensors.{_sensor_idx}.latest_sent_value': dates[j - 1]}})
                            # count pushed values in whole process
                            collection.update({"_id": 2}, {"$inc": {"valueCount": len(messages)}})
                            valuebulk['collapsedMessages'] = []
                            messages = valuebulk['collapsedMessages']
                        else:
                            valuebulk['collapsedMessages'] = []
                            messages = valuebulk['collapsedMessages']
                            continue  # this should probably be an exception...

        def _update(_measurand: str, _func: Callable):
            _sensors = mongo_sensors_by_local_id(dwd_id, _measurand)
            _chunks = seperate_by_sensor(dates, _sensors, dwd_id)
            _process_chunks(air_temperature_idx, _chunks, _sensors, _measurand, _func)

        if measurand == 'temperature':
            if air_temperature_idx is not None: _update('temperature', _add_float_values)
            if humidity_idx is not None: _update('humidity', _add_float_values)
        elif measurand == 'cloudiness':
            if cloudiness_idx is not None: _update('cloudiness', _add_cloudiness_values)
        elif measurand == 'air_pressure':
            if air_pressure_idx is not None: _update('air_pressure', _add_float_values)
            if air_pressure_nn_idx is not None: _update('air_pressure_nn', _add_float_values)
        elif measurand == 'wind_speed':
            if wind_speed_idx is not None: _update('wind_speed', _add_float_values)
            if wind_direction_idx is not None: _update('wind_direction', _add_float_values)

    except Exception as e:
        print("Exception {}".format(e))
    finally:
        secretmanager.complete_sequence(rest_names)
    return {"message": "finished"}
