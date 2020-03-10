#!/usr/bin/env python

"""osnapi.py: This module is a wrapper for opensense.network's API"""

__author__ = "Florian Peters https://github.com/flpeters"

from typing import List, Dict, Union

import requests

Sensor = Dict[str, Union[int, str, Dict[str, float]]]
SensorWithValue = Dict[str, Union[int, str, Dict[str, float], Dict[str, Union[str, float]]]]
Value = Measurand = Unit = Dict[str, Union[str, float]]
Liscense = Dict[str, Union[int, str, bool]]

__all__ = ['Settings',
           'getSensors', 'getSensor', 'addSensor', 'deleteSensor', 'mySensors', 'mySensorIds',
           'getFirstLastValueForSensor', 'getValues', 'getValuesForSensor', 'addValue', 'addMultipleValues',
           'login', 'profile',
           'getMeasurands', 'getMeasurand',
           'getLicenses', 'getLicense',
           'getUnits', 'getUnit']


class Settings:
    api_endpoint = "https://dep2.osn.k8s.ise-apps.de/api/v1.0"
    username = None
    password = None
    auth_token = None

    def __repr__(self):
        return f'api_endpoint:\t{self.api_endpoint}\nusername:\t{self.username}\npassword:\t{self.password}\nauth_token:\t{self.auth_token}'


#######################################
#               HELPERS               #
#######################################

def generate_headers(requires_auth: bool) -> Dict:
    headers = {'accept': 'application/json',
               'accept-encoding': 'gzip, deflate',
               'content-type': 'application/json',
               'cache-control': 'no-cache'}
    if requires_auth:
        headers['Authorization'] = Settings.auth_token
    return headers


def handle_response(query: str, response: requests.Response) -> Union[Dict, str]:
    try:
        text = response.json()
    except:
        text = response.text
    if response.status_code == 200:
        return text
    elif response.status_code == 500:
        info = f'The Server has encountered an unexpected problem, probably due to you attempting something that requires authorization. Try logging in and repeating the Request.\
            \n--Status Code   : {response.status_code}\
            \n--Request to    : {query}\
            \n--Response Body : {text}'
        raise PermissionError(info)
    elif response.status_code == 408:
        info = f'The Server has closed this connection, probably due to the request being too large. Try sending less data at once.\
            \n--Status Code   : {response.status_code}\
            \n--Request to    : {query}\
            \n--Response Body : {text}'
        raise TimeoutError(info)
    else:
        info = f'Something went wrong with your request.\
            \n--Status Code   : {response.status_code}\
            \n--Request to    : {query}\
            \n--Response Body : {text}'
        raise Exception(info)


def send_get(query: str, requires_auth: bool = False) -> Dict:
    headers = generate_headers(requires_auth)
    response = requests.get(url=query,
                            headers=headers)
    return handle_response(query, response)


def send_post(query: str, body: Dict, requires_auth: bool = False) -> Dict:
    headers = generate_headers(requires_auth)
    response = requests.post(url=query,
                             json=body,
                             headers=headers)
    return handle_response(query, response)


def send_delete(query: str, requires_auth: bool = False) -> Dict:
    headers = generate_headers(requires_auth)
    response = requests.delete(url=query,
                               headers=headers)
    return handle_response(query, response)


def build_query(target: str, **kwargs):
    query = f'{Settings.api_endpoint}{"" if target.startswith("/") else "/"}{target}?'
    for key, value in kwargs.items():
        if value and key != 'self': query += f'{key}={value}&'
    return query


#######################################
#               LOGIN                 #
#######################################

def login(username: str, password: str) -> str:
    query = build_query(target='/users/login')
    body = {"username": username, "password": password}
    response = send_post(query, body)
    Settings.username, Settings.password = username, password
    Settings.auth_token = response['id']
    return response['id']


#######################################
#              SENSORS                #
#######################################

def getSensors(measurandId: int = None,
               refPoint: List[float] = None,
               maxDistance: float = None,
               numNearest: int = None,
               boundingBox: List[float] = None,
               boundingPolygon: List[float] = None,
               minAccuracy: int = None,
               maxAccuracy: int = None,
               maxSensors: int = None,
               allowsDerivatives: bool = None,
               allowsRedistribution: bool = None,
               requiresAttribution: bool = None,
               requiresChangeNote: bool = None,
               requiresShareAlike: bool = None,
               requiresKeepOpen: bool = None) -> List[Sensor]:
    args = locals()
    query = build_query(target='/sensors', **args)
    return send_get(query)


def getSensor(id: int) -> Sensor:
    query = build_query(target=f'/sensors/{id}')
    return send_get(query)


def addSensor(body: Sensor) -> Sensor:
    query = build_query(target='/sensors/addSensor')
    return send_post(query, body, requires_auth=True)


def deleteSensor(id: int) -> str:
    query = build_query(target=f'/sensors/{id}')
    return send_delete(query, requires_auth=True)


def mySensors() -> List[Sensor]:
    query = build_query(target='/sensors/mysensors')
    return send_get(query, requires_auth=True)


def mySensorIds() -> List[int]:
    query = build_query(target='/sensors/mysensorids')
    return send_get(query, requires_auth=True)


#######################################
#                VALUES               #
#######################################

def getFirstLastValueForSensor(id: int,
                               first: bool,
                               last: bool) -> SensorWithValue:
    if first and last:
        query = build_query(target=f'/sensors/{id}/values/firstlast')
    elif first:
        query = build_query(target=f'/sensors/{id}/values/first')
    elif last:
        query = build_query(target=f'/sensors/{id}/values/last')
    else:
        raise Exception(f'At least one of the options has to be true:\
        \nfirst: {first}\nlast: {last}')
    return send_get(query)


def getValues(measurandId: int = None,
              refPoint: List[float] = None,
              maxDistance: float = None,
              boundingBox: List[float] = None,
              boundingPolygon: List[float] = None,
              maxSensors: int = None,
              minTimestamp: str = None,
              maxTimestamp: str = None,
              aggregationType: str = None,
              aggregationRange: str = None,
              minValue: float = None,
              maxValue: float = None,
              allowsDerivatives: bool = None,
              allowsRedistribution: bool = None,
              requiresAttribution: bool = None,
              requiresChangeNote: bool = None,
              requiresShareAlike: bool = None,
              requiresKeepOpen: bool = None) -> List[SensorWithValue]:
    args = locals()
    query = build_query(target='/values', **args)
    return send_get(query)


def getValuesForSensor(id: int,
                       minTimestamp: str = None,
                       maxTimestamp: str = None,
                       aggregationType: str = None,
                       aggregationRange: str = None,
                       minValue: float = None,
                       maxValue: float = None) -> SensorWithValue:
    args = locals()
    query = build_query(target=f'/sensors/{args.pop("id")}/values', **args)
    return send_get(query)


def addValue(body: Value) -> str:
    query = build_query(target='/sensors/addValue')
    return send_post(query, body, requires_auth=True)


def addMultipleValues(body: Dict[str, List[Value]]) -> str:
    query = build_query(target='/sensors/addMultipleValues')
    return send_post(query, body, requires_auth=True)


#######################################
#                USERS                #
#######################################

def profile() -> List[Dict[str, Union[str, int]]]:
    query = build_query(target='/users/profile')
    return send_get(query, requires_auth=True)


#######################################
#              MEASURANDS             #
#######################################

def getMeasurands(name: str = None) -> List[Measurand]:
    args = locals()
    query = build_query(target='/measurands', **args)
    return send_get(query)


def getMeasurand(id: int) -> Measurand:
    query = build_query(target=f'/measurands/{id}')
    return send_get(query)


#######################################
#               LICENSES              #
#######################################

def getLicenses(shortName: str = None,
                allowsDerivatives: bool = None,
                allowsRedistribution: bool = None,
                requiresAttribution: bool = None,
                requiresChangeNote: bool = None,
                requiresShareAlike: bool = None,
                requiresKeepOpen: bool = None) -> List[Liscense]:
    args = locals()
    query = build_query(target='/licenses', **args)
    return send_get(query)


def getLicense(id: int) -> Liscense:
    query = build_query(target=f'/licenses/{id}')
    return send_get(query)


#######################################
#                UNITS                #
#######################################

def getUnits(name: str = None,
             measurandId: int = None) -> List[Unit]:
    args = locals()
    query = build_query(target='/units', **args)
    return send_get(query)


def getUnit(id: int) -> Unit:
    query = build_query(target=f'/units/{id}')
    return send_get(query)
