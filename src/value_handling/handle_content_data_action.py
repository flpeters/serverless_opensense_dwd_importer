"""osnapi.py: This module is a wrapper for opensense.network's API"""

__author__ = "Florian Peters https://github.com/flpeters"

import time
from contextlib import contextmanager
from datetime import datetime
from typing import List, Callable

try: import osnapi as api
except: import deployment_tmp.osnapi as api
    
try: import secretmanager
except: import deployment_tmp.secret_manager as secretmanager

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import ConnectionFailure

mongo_db_url = secretmanager.__MONGOURL__

################## General Helpers ##################
def clean_str(line: str) -> List[str]: return ''.join(line.split()).split(';')

def to_float_or_none(x:str) -> float:
    try: return float(x)
    except ValueError as e:
        print(f'ValueError: {e}')
        return None

def to_int_or_none(x:str) -> int:
    try: return int(x)
    except ValueError as e:
        print(f'ValueError: {e}')
        return None
    
def batchify(a:int, b:int, bs:int) -> List[tuple]:
    """Convert a index range [a:b] into multiple batches, each no larger than bs."""
    return [(i, min(b, i+bs)) for i in range(a, b, bs)]

def to_iso_date(timestamp:str, format:str) -> str: return datetime.strptime(timestamp, format).isoformat()

def iso_to_int(iso_date:str):
    """convert a ISO 8601 string to a unique int"""
    year = iso_date[0:4]
    month = iso_date[5:7]
    day = iso_date[8:10]
    hour = iso_date[11:13]
    minute = iso_date[14:16]
    second = iso_date[17:19]
    return int(f'{year}{month}{day}{hour}{minute}{second}')


################## Opensense ##################
# TODO(florian): This can be cleaned up due to changed to the api.
def osn_push_valuebulk(valuebulk: dict) -> bool:
    try: return api.addMultipleValues(body=valuebulk) == 'OK'

    except PermissionError as pe:
        print(f'Failed pushing values once, due to permission error.\n\t--> sending login and retrying second time.')
        try: api.login(username=secretmanager.__OSNUSERNAME__, password=secretmanager.__OSNPASSWORD__)
        except Exception as e2: print(f'WARNING: Failed to log in ({e2}).\n\t--> Skipping values.')

    except TimeoutError as te: print(f'Failed pushing values once, due to timeout error.\n\t--> retrying')
    except Exception as e: print(f'Failed pushing values once, due to unhandled error.\n\t--> retrying')

    for x in range(2, 5):
        try: return api.addMultipleValues(body=valuebulk) == 'OK'
        except Exception as e:
            print(f'WARNING: Failed pushing values a {x}(nd/th) time of maximum 5 retries({e}).\n\t--> retrying.')

    return False


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
        
# NOTE(florian): merge_already_sent() is defined down below
def mongo_merge_all_already_sent(local_id:str, time_class:str, collection:Collection):
    """merges all sent_values by date"""
    for sensor in collection.find_one(filter={'local_id' : local_id})['sensors']:
        collection.update_one(filter={'local_id' : local_id, 'sensors.idx' : sensor['idx']},
                              update={'$set':
                                      {f'sensors.$.sent_values': merge_already_sent(sensor['sent_values'], time_class)}})
        
def mongo_sensors_by_local_id_merged_already_sent(local_id:str, time_class:str, collection:Collection):
    """returns all sensors of a particular local_id, but also merges all sent_values by date, before returning."""
    mapping = collection.find_one(filter={'local_id': local_id})
    if mapping is not None:
        sensors = mapping['sensors']
        for sensor in sensors:
            sensor['sent_values'] = merge_already_sent(sensor['sent_values'], time_class)
            collection.update_one(filter={'local_id' : local_id, 'sensors.idx' : sensor['idx']},
                                  update={'$set': {f'sensors.$.sent_values': sensor['sent_values']}})
    else: raise Exception(f'No sensor mapping found for local id: {local_id}')
    return sorted(sensors, key=lambda x: x['earliest_day'])

        
################## Specialized Helpers ##################
def get_indices(fieldDefs:list) -> tuple:
    """Look up the indices of a bunch of expected fields."""
    def _idx_of(*args):
        for s in args:
            if s in fieldDefs: return fieldDefs.index(s)
    # Meta Information
    stationIDIndex = _idx_of('STATIONS_ID')
    dateIndex      = _idx_of('MESS_DATUM')
    qualityIndex   = _idx_of('QUALITAETS_NIVEAU', 'QN_9', 'QN_8', 'QN_7', 'QN_3')
    structure_version_index  = _idx_of('STRUKTUR_VERSION')
    # Content Data
    airTemperatureIndex      = _idx_of('LUFTTEMPERATUR', 'TT_TU')
    humidityIndex            = _idx_of('REL_FEUCHTE', 'RF_TU')
    cloudinessIndex          = _idx_of('GESAMT_BEDECKUNGSGRAD', 'V_N') # values between 1 and 8
    precipitationYesNoIndex  = _idx_of('NIEDERSCHLAG_GEFALLEN_IND', 'RS_IND')  # boolean: 0->no prec., 1->prec.
    precipitationAmountIndex = _idx_of('NIEDERSCHLAGSHOEHE', 'R1')  # value in mm
    precipitationTypeIndex   = _idx_of('NIEDERSCHLAGSFORM', 'WRTR')  # 9 is exceptionValue, valid values between 1 and 8
    airPressureNNIndex       = _idx_of('LUFTDRUCK_REDUZIERT', 'P')
    airPressureIndex         = _idx_of('LUFTDRUCK_STATIONSHOEHE', 'P0')
    sunshineMinsPerHourIndex = _idx_of('STUNDENSUMME_SONNENSCHEIN', 'SD_SO')
    windSpeedIndex           = _idx_of('WINDGESCHWINDIGKEIT', 'F')
    windDirectionIndex       = _idx_of('WINDRICHTUNG', 'D')
    return (stationIDIndex, dateIndex, qualityIndex,
            structure_version_index, airTemperatureIndex,
            humidityIndex, cloudinessIndex,
            precipitationYesNoIndex,
            precipitationAmountIndex, precipitationTypeIndex,
            airPressureNNIndex, airPressureIndex,
            sunshineMinsPerHourIndex,
            windSpeedIndex, windDirectionIndex)

def belongs_to_sensor(a:str, ts:str, b:str) -> bool:
    """checks if ts is between a and b"""
    return (a <= ts and (ts <= b or b == ''))

def find_transition(start:int, end:int, List:list, condition:Callable) -> int:
    """Uses a Binary Search approach to find the index of the first element where condition is no longer true."""
    # NOTE(florian): The reason for using the first "wrong" element, is that slicing "[i:j]" excludes the j'th value.
    pivot = (start + end) // 2
    while end - start > 1:
        if condition(List[pivot]):
            if not condition(List[pivot + 1]): return pivot + 1
            else: start, pivot = pivot, (pivot + end) // 2 # move to the right
        else:
            if condition(List[pivot - 1]): return pivot
            else: end, pivot = pivot, (start + pivot) // 2 # move to the left

def seperate_by_sensor(dates:list, sensors:list) -> dict:
    """Splits a list of timestamped values into chunks depending on what sensor the value was recorded by"""
    chunks, start, ld = {}, 0, len(dates)
    for i, sensor in enumerate(sensors):
        a, b  = sensor['earliest_day'], sensor['latest_day']
        ts    = dates[start]
        if belongs_to_sensor(a, ts, b):
            ts = dates[-1]
            if ts <= b or b == '': # fast path, entire remaining list belongs to this sensor
                chunks[i] = (start, ld)
                break
            else: # find the end of this sensors interval, and continue with the next sensor
                pivot = find_transition(start, ld - 1, dates, lambda x: belongs_to_sensor(a, x, b))
                chunks[i] = (start, pivot)
                start = pivot
                continue
        else: # the current position does not belong to this sensor
            if ts < a: # either check whether to skip over some values, or skip this sensors interval
                ts = dates[-1]
                if a < ts: # check if some values later on still belong to this sensor
                    if belongs_to_sensor(a, ts, b): # from some point onwards, the remaining list belongs to this senso
                        pivot = find_transition(start, ld - 1, dates, lambda x: not belongs_to_sensor(a, x, b))
                        chunks[i] = (pivot, ld)
                        break
                    else: # end of the list is not part of this sensor, there might be a subsection though
                        for j, ts in enumerate(dates[start:]):
                            if b < ts: # end of potential interval reached, skip to next sensor
                                start = start + j
                                break
                            else:
                                if belongs_to_sensor(a, ts, b): # start of this sensors interval found
                                    pivot = find_transition(start, ld - 1, dates,
                                                            lambda x: belongs_to_sensor(a, x, b))
                                    chunks[i] = (start + j, pivot)
                                    start = pivot
                                    break
                else: break # the last element in the list is still before this sensors interval
            else: continue  # the current list position is after this sensors interval
    return chunks

def _lies_within_strict(f, x, t): return f <= x <= t # example
def _lies_within_plus_one(f, x, t): return f <= x <= t + 1 # example
def _lies_within_iso_str(f:str, x:str, t:str) -> bool:
    fi, xi, ti = iso_to_int(f), iso_to_int(x), iso_to_int(t)
    return fi <= xi and ((xi-ti) % 760000) <= 10000 # one hour mod rollover for one day

def matcher_by_time_class(time_class:str) -> Callable: # TODO(florian): Add more time_classes
    return {'hourly' : _lies_within_iso_str}.get(time_class, None)
# NOTE(florian): Using _lies_within_strict as a default wont lead to wrong results, but it'll slow things down over time,
# because more and more timestamps will accumulate in mongodb

# TODO(florian): refactor this
def merge_already_sent(sub_chunks:List[Tuple[Union[str, int]]],
                       time_class:str=None) -> list:
    _lies_within = matcher_by_time_class(time_class)
    if _lies_within is None: # TODO(florian): should this crash, instead of a warning?
        print(f'WARNING: Using the default strict comparison for merging already sent values because time_class is not recognised: {time_class}')
        _lies_within = _lies_within_strict
    if len(sub_chunks) > 1:
        sub_chunks = sorted(sub_chunks, key=lambda x: x[0])
        out_chunks = []
        f, t = sub_chunks[0]
        for _f, _t in sub_chunks[1:]:
            if _lies_within(f, _f, t): t = max(t, _t)
            else:
                out_chunks.append((f, t))
                f, t = _f, _t
        out_chunks.append((f, t))
        return out_chunks
    else: return sub_chunks
    
def split_by_already_sent(start:int, end:int, timestamps:List[str], already_sent:List[Tuple[str]]) -> List[int]:
    """Check which of the timestamps lie outside the ranges of the already_sent timestamps, and return them."""
    # NOTE(florian): (end - 1) is used, because end is the first element that is NO LONGER PART OF the data, so we don't include it.
    if len(already_sent) > 0:
        yet_to_sent = []
        for a, b in already_sent:
            if timestamps[start] < a: # is start before the start of the already_sent interval?
                if a < timestamps[end - 1]: # is end after the start of the already_sent interval?
                    # since both are true, there has to be a transition.
                    yet_to_sent.append((start, find_transition(start, end - 1, timestamps, lambda x: x < a)))
                else:
                    yet_to_sent.append((start, end)) # no part of the data has been sent yet
                    break
            else: pass # start has already been sent
            
            if timestamps[start] < b < timestamps[end - 1]:
                # there is still data after this already_sent interval
                start = find_transition(start, end - 1, timestamps, lambda x: x <= b)
            else:
                if b < timestamps[end - 1]: continue # this already_sent interval is entirely before the data
                else: break # subsequent already_sent intervals all come after this one chronologically
        else: # the loop has run to completion, without hitting a break. check if there is still data left
            if start < end: yet_to_sent.append((start, end))
        return yet_to_sent
    else: return [(start, end)]

##################### MAIN #######################
def handle_content_data(first_line:str,
                        lines     :List[str],
                        measurand :str='temperature', # TODO(florian): pass these from somewhere
                        data_class:str='recent',
                        time_class:str='hourly'):
    logged_action = False # NOTE(florian): needed?
    first_line = clean_str(first_line)
    len_first_line = len(first_line)
    field_defs = get_indices(first_line)
    print(field_defs)

    if len_first_line < 5: raise Exception(f'Nr of fields is lower than expected: {first_line}')

    dwd_id_idx, date_idx, quality_idx , structure_version_idx, \
    air_temperature_idx , humidity_idx, cloudiness_idx, \
    precipitation_yes_no_idx, precipitation_amount_idx, precipitation_type_idx, \
    air_pressure_nn_idx     , air_pressure_idx, \
    sunshine_mins_per_hour_idx, wind_speed_idx, wind_direction_idx = field_defs

    if dwd_id_idx is None: raise Exception(f'File does not contain a dwd_id index: {field_defs}')
    if date_idx is None: raise Exception(f'File does not contain a Timestamp index: {field_defs}')
    if quality_idx is None: pass # Not Implemented yet and not essential
    if structure_version_idx is None: pass # Not Implemented yet and not essential

    print(f'nr of lines at start: {len(lines)}')
    lines = [clean_str(x) for x in lines if not(x is None or x == '')]

    dwd_id = lines[0][dwd_id_idx]
    if not dwd_id.isdigit() or dwd_id is None:
        dwd_id = lines[1][dwd_id_idx]
        if not dwd_id.isdigit() or dwd_id is None: raise Exception(f'Could not find a valid dwd_id: {dwd_id}')
    print(f'dwd_id: {dwd_id}')

    lines = sorted(lines, key=lambda x: x[date_idx])
    lines = [line for line in lines if (len(line) == len_first_line and
                                        line[dwd_id_idx] == dwd_id)]
    print(f'nr of lines after removing invalids: {len(lines)}')

    lines = list(zip(*lines)) # transpose

    # https://docs.python.org/3/library/datetime.html#strftime-strptime-behavior
    iso_dates = [to_iso_date(timestamp=ts, format='%Y%m%d%H') for ts in lines[date_idx]]

    print('-'*80)

    valuebulk = {'collapsedMessages': []}
    messages = valuebulk['collapsedMessages']

    def _add_float_values(i:int, j:int,
                          idx:int, osn_id:int):
        for iso_date, value in zip(iso_dates[i:j], lines[idx][i:j]):
            value = to_float_or_none(value)
            if value is not None and value != -999.0:
                messages.append({'sensorId': osn_id,
                                 'timestamp': iso_date,
                                 'numberValue': value})

    def _add_cloudiness_values(i:int, j:int,
                               idx:int, osn_id:int):
        for iso_date, value in zip(iso_dates[i:j], lines[idx][i:j]):
            value = to_int_or_none(value)
            if value is not None and 0 < value < 8:
                value *= 0.125 # 1/8 = 0.125 -> map to float between 0 and 1
                messages.append({'sensorId': osn_id,
                                 'timestamp': iso_date,
                                 'numberValue': value})
                
    def _process_chunks(idx:int, chunks:tuple, sensors:dict, local_id:str, _add_values:Callable, collection:Collection):
        nonlocal valuebulk, messages, logged_action
        if not logged_action:
            collection.update({"_id": 5}, {"$inc": {"actionCount": 1}}) # NOTE(florian): needed?
            logged_action = True
        print(chunks)
        for sensor_id in chunks:
            sensor       = sensors[sensor_id]
            osn_id       = sensor['osn_id']
            sensor_idx   = sensor['idx']
            already_sent = sensor['sent_values']
            for yet_to_be_sent in split_by_already_sent(*chunks[sensor_id], iso_dates, already_sent):
                for i, j in batchify(*yet_to_be_sent, max_batch_size=2000): # TODO(florian): Make max_batch_size global?
                    _add_values(i=i, j=j, idx=idx, osn_id=osn_id)
                    t0 = time.time()
                    collection.update({"_id": 2}, {"$inc": {"aimedValueCount": len(messages)}}) # NOTE(florian): needed?
                    if osn_push_valuebulk(valuebulk):
                        print(f'Pushed {len(messages)} values to osn_id {osn_id}. took: {round(time.time() - t0, 5)} sec')
                        resp = collection.update_one(filter={'local_id' : local_id, 'sensors.idx' : sensor_idx},
                                              update={'$addToSet':
                                                      {f'sensors.$.sent_values': (iso_dates[i], iso_dates[j - 1])}})
                        if not resp.acknowledged:
                            print(f'WARNING: Failed to record successful push on mongodb! {local_id} {sensor_idx} {iso_dates[i]}')
                        collection.update({"_id": 2}, {"$inc": {"valueCount": len(messages)}}) # NOTE(florian): needed?
                        valuebulk['collapsedMessages'] = []
                        messages = valuebulk['collapsedMessages']
                    else:
                        valuebulk['collapsedMessages'] = []
                        messages = valuebulk['collapsedMessages']
                        continue
        mongo_merge_all_already_sent(local_id, time_class, collection)

    def _update(_measurand:str, _idx:int, _func:Callable):
        local_id = f'{dwd_id}-{_measurand}'
        with mongo_conn(mongo_db_url) as collection:
            sensors = mongo_sensors_by_local_id_merged_already_sent(local_id, time_class, collection)
            chunks = seperate_by_sensor(iso_dates, sensors)
            _process_chunks(_idx, chunks, sensors, local_id, _func, collection)

    if measurand == 'temperature':
        if air_temperature_idx is not None: _update('temperature'    , air_temperature_idx, _add_float_values)
        if humidity_idx        is not None: _update('humidity'       , humidity_idx       , _add_float_values)
    elif measurand == 'cloudiness':
        if cloudiness_idx      is not None: _update('cloudiness'     , cloudiness_idx     , _add_cloudiness_values)
    elif measurand == 'air_pressure':
        if air_pressure_idx    is not None: _update('air_pressure'   , air_pressure_idx   , _add_float_values)
        if air_pressure_nn_idx is not None: _update('air_pressure_nn', air_pressure_nn_idx, _add_float_values)
    elif measurand == 'wind_speed':
        if wind_speed_idx      is not None: _update('wind_speed'     , wind_speed_idx     , _add_float_values)
        if wind_direction_idx  is not None: _update('wind_direction' , wind_direction_idx , _add_float_values)
    
##################### OpenWhisk Entrypoint #######################
def main(args):
    csv = args.get("csv")
    rest_names = args.get("restfilenames")
    measurand = args.get("measurand", 'temperature')
    if csv is None: return {"error": "seuquence should be stopped"}
    try:
        lines = csv.splitlines()
        # first_line, lines = lines[0], lines[1:]
        first_line = lines.pop(0)
        lines.pop(0) # NOTE(florian): why pop(0) here? Above should work fine
        handle_content_data(first_line=first_line, lines=lines)
    except Exception as e: print("Exception {}".format(e))
    finally: secretmanager.complete_sequence(rest_names)
    return {"message": "finished"}