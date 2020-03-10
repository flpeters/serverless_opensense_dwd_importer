#!/usr/bin/python3

"""wsksetup.py: This program starts up a local OpenWhisk environment by automating the devtools startup"""

__author__ = "Ahmet Kilic https://github.com/flamestro"

import json
import os
import sys

import deployment_tmp.config_template as conf_template

arguments = sys.argv[1:]
config_path = ""


def init_args():
    global config_path
    try:
        config_index = arguments.index("--config")
        config_path = arguments[config_index + 1]
        if not os.path.isfile(config_path):
            with open(config_path, 'w+') as file:
                json_data = conf_template
                json.dump(json.loads(json_data), file)
                print("Created config file under {} please enter informations to continue".format(config_path))
                sys.exit(0)
    except:
        root_dir = os.path.abspath(__file__)
        for _ in range(2):
            root_dir = os.path.dirname(root_dir)
        config_path = root_dir + "/data/config.json"
init_args()

with open(config_path, "r") as f:
    raw = f.read()
    config = json.loads(raw)
    openwhisk_devtools_path = config["DEVTOOLS"]

os.system("cd " + openwhisk_devtools_path + "docker-compose && sudo make run && cp .wskprops " + config["WSKPROPSPATH"] + ".wskpropsLOCAL")
