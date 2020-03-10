#!/usr/bin/python3

"""
wskshutdown.py: This program shuts down an locally running OpenWhisk environment by automating the devtools shutdown
"""

__author__ = "Ahmet Kilic https://github.com/flamestro"

import json
import os
import sys

arguments = sys.argv[1:]
config_path = ""


def init_args():
    global config_path
    try:
        config_index = arguments.index("--config")
        config_path = arguments[config_index + 1]
    except ValueError:
        root_dir = os.path.abspath(__file__)
        for _ in range(2):
            root_dir = os.path.dirname(root_dir)
        config_path = root_dir + "/data/config.json"

init_args()

with open(config_path, "r") as f:
    raw = f.read()
    openwhisk_devtools_path = json.loads(raw)["DEVTOOLS"]
os.system("cd " + openwhisk_devtools_path + "docker-compose && sudo make destroy")