#!/usr/bin/python3

"""
secret_manager.py: This module should concentrate all secrets and action call methods in one place, this should make
it easier to test functions and mock responses
"""

__author__ = "Ahmet Kilic https://github.com/flamestro"


__URLAPI__ = '"URLAPI"'
__URLAPINOWEB__ = '"URLAPINOWEB"'
__OPENWHISKUSERNAME__ = '"OPENWHISKUSERNAME"'
__OPENWHISKPWD__ = '"OPENWHISKPWD"'
__OSNUSERNAME__ = '"OSNUSERNAME"'
__OSNPASSWORD__ = '"OSNPASSWORD"'
__MONGOURL__ = '"MONGOURL"'

import requests


def complete_sequence(rest_filenames):
    filename = rest_filenames[0]
    rest_names = rest_filenames[1:]
    if not rest_names[0].startswith("end"):
        try:
            response = requests.post(__URLAPINOWEB__ + "completesequenceaction",
                                     auth=(__OPENWHISKUSERNAME__, __OPENWHISKPWD__),
                                     json={"filename": filename,
                                           "restfilenames": rest_names},
                                     verify=False)
            print(response)
        except Exception as e:
            print("could not jump to next file restfiles are {} Exception is {}".format(rest_names, e))
            pass
    else:
        print("finished sequence")
    return response


def get_filename_list_action(path="climate_environment/CDC/observations_germany/climate/hourly/air_temperature/recent/"):
    namelist = requests.get(__URLAPI__ + "getfilenamesaction.json",
                            data={
                                "path": path},
                            verify=False).json()
    return namelist
