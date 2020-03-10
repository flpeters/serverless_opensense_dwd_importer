#!/usr/bin/python3

"""deleteactions.py: This program should easify the deletion of OpenWhisk Actions which are mentioned in a given conf"""

__author__ = "Ahmet Kilic https://github.com/flamestro"

import json
import os
import sys

arguments = sys.argv[1:]

# you could use argparse to do this somehow, but the flexibility in using this approach wins here
clistart = None
config_path = ""


def init_args():
    global clistart, config_path
    try:
        config_index = arguments.index("--config")
        config_path = arguments[config_index + 1]
        print(config_path)
    except ValueError:
        root_dir = os.path.abspath(__file__)
        for _ in range(2):
            root_dir = os.path.dirname(root_dir)
        config_path = root_dir + "/data/config.json"
        print(config_path)
    try:
        deployment_option_index = arguments.index("--deployment")
        if arguments[deployment_option_index + 1] == "IBM":
            clistart = "ibmcloud fn "
        elif arguments[deployment_option_index + 1] == "REMOTE":
            clistart = "wsk "
        else:
            clistart = "wsk -i "
    except ValueError:
        pass


init_args()

with open(config_path, "r") as f:
    raw = f.read()
    config = json.loads(raw)


def substring_maker(inputstring, start, end, index=0):
    return (inputstring.split(start))[-1].split(end)[index]


all_files = [obj["filename"] for obj in config["ACTIONMAPPINGS"]]
for actionfile in all_files:
    actionname = substring_maker(actionfile, "/", ".py")
    os.system("{}action delete {}".format(clistart, config["ACTIONMAPPINGS"][all_files.index(actionfile)]["actionname"]))

# sequence meta data
os.system("{}action delete metasequenceaction".format(clistart))

# sequence value data
os.system("{}action delete valuesequenceaction".format(clistart))

# whole sequence
os.system("{}action delete completesequenceaction".format(clistart))
