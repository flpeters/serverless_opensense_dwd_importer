#!/usr/bin/python3

"""autodeploy.py: This program should easify the deployment of OpenWhisk Actions which are mentioned in a given conf"""

__author__ = "Ahmet Kilic https://github.com/flamestro"

import fileinput
import hashlib
import json
import os
import sys
from enum import Enum

import deployment_tmp.config_template as conf_template


class DeploymentOption(Enum):
    NODEPLOYMENT = 0
    IBM = 1
    REMOTE = 2
    LOCAL = 3


program_name = sys.argv[0]
arguments = sys.argv[1:]
clistart = None
config_path = ""
deployment = DeploymentOption.NODEPLOYMENT
fresh_start = None


def init_args():
    global clistart, config_path, deployment, fresh_start
    try:
        config_index = arguments.index("--config")
        config_path = arguments[config_index + 1]
        if not os.path.isfile(config_path):
            with open(config_path, 'w+') as file:
                json_data = conf_template
                json.dump(json.loads(json_data), file)
                print("Created config file under {} please enter informations to continue".format(config_path))
                sys.exit(0)
    except ValueError:
        root_dir = get_root_dir()
        config_path = root_dir + "/data/config.json"
        print(config_path)
    try:
        deployment_option_index = arguments.index("--deployment")
        if arguments[deployment_option_index + 1].lower() == "ibm":
            deployment = DeploymentOption.IBM
            clistart = "ibmcloud fn "
        elif arguments[deployment_option_index + 1].lower() == "remote":
            deployment = DeploymentOption.REMOTE
            clistart = "wsk "
        else:
            deployment = DeploymentOption.LOCAL
            clistart = "wsk -i "
    except ValueError:
        deployment = DeploymentOption.LOCAL
        clistart = "wsk -i "
    finally:
        print("Deploying " + deployment.name)
    try:
        fresh_start_index = arguments.index("--fresh")
        print(arguments[fresh_start_index + 1].lower())
        if arguments[fresh_start_index + 1].lower() == "true":
            os.system(
                'docker run --rm -v "$PWD:/tmp" openwhisk/python3action bash -c "cd tmp && virtualenv virtualenv && source virtualenv/bin/activate && pip install -r requirements_for_actions.txt"')
            fresh_start = True
        elif arguments[fresh_start_index + 1].lower() == "false":
            fresh_start = False
        else:
            # check if actions are already created and if yes assume that it should be a fresh start
            output = os.popen('{}list'.format(clistart)).read()

            if len(output.split("getfilenamesaction")) > 1:
                fresh_start = False
            else:
                fresh_start = True
    except ValueError:
        # check if actions are already created and if yes assume that it should be a fresh start
        output = os.popen('{}list'.format(clistart)).read()

        if len(output.split("placeholderaction")) > 1:
            fresh_start = False
        else:
            fresh_start = True
        pass


def get_root_dir():
    root_dir = os.path.abspath(__file__)
    for _ in range(2):
        root_dir = os.path.dirname(root_dir)
    return root_dir


init_args()

with open(config_path, "r") as f:
    raw = f.read()
    config = json.loads(raw)
    os.system('cp ' + config["WSKPROPSPATH"] + ".wskprops" + deployment.name + " ~/.wskprops")


def substring_maker(inputstring, start, end, index=0):
    return (inputstring.split(start))[-1].split(end)[index]


# hash files to see if they changed and only update changed actions
def get_digest(file_path):
    h = hashlib.sha256()

    with open(file_path, 'rb') as file:
        while True:
            # Reading is buffered, so we can read smaller chunks.
            chunk = file.read(h.block_size)
            if not chunk:
                break
            h.update(chunk)

    return h.hexdigest()


# create urlstart for noweb actions
def getURL(web=False):
    if web:
        web = "--web true"
    else:
        web = ""
    print("get api endpoint urlstart by creating a placeholder action")
    os.system('cp ' + get_root_dir() + "/" + config["ACTIONMAPPINGS"][0]["filename"] + " __main__.py")
    os.system("{}action delete {}".format(clistart, config["ACTIONMAPPINGS"][0]["actionname"]))
    os.system("zip -r {}.zip virtualenv __main__.py > /dev/null".format(config["ACTIONMAPPINGS"][0]["actionname"]))

    os.system("{0}action create {1} --kind python:3 {1}.zip ".format(clistart,
                                                                     config["ACTIONMAPPINGS"][0]["actionname"]) + web)
    os.system("rm {}.zip".format(config["ACTIONMAPPINGS"][0]["actionname"]))

    output = os.popen('{}action get {} --url'.format(clistart, config["ACTIONMAPPINGS"][0]["actionname"])).read()
    start = 'https'
    end = config["ACTIONMAPPINGS"][0]["actionname"]
    return "https" + substring_maker(output, start, end)


urlstart_noweb = getURL()
urlstart = getURL(True)
all_files = [get_root_dir() + "/" + obj["filename"] for obj in config["ACTIONMAPPINGS"]]

if fresh_start:
    for file in all_files:
        config["FILEHASHES"][file] = get_digest(file)

# init secretmanager module
os.system("cp secret_manager.py secretmanager.py")
for line in fileinput.input("secretmanager.py", inplace=True):
    line = line.replace('"OSNUSERNAME"', config["OSNUSERNAME"])
    line = line.replace('"OSNPASSWORD"', config["OSNPASSWORD"])
    line = line.replace('"MONGOURL"', config["MONGOURL" + deployment.name])
    line = line.replace('"OPENWHISKPWD"', config["OPENWHISKPWD" + deployment.name])
    line = line.replace('"OPENWHISKUSERNAME"', config["OPENWHISKUSERNAME" + deployment.name])
    line = line.replace('"URLAPINOWEB"', urlstart_noweb)
    line = line.replace('"URLAPI"', urlstart)
    sys.stdout.write(line)

for actionfile in all_files:
    if get_digest(actionfile) != config["FILEHASHES"][actionfile] or fresh_start:
        actionname = substring_maker(actionfile, "/", ".py")
        print("start process for {}".format(actionname))

        actionname = config["ACTIONMAPPINGS"][all_files.index(actionfile)]["actionname"]
        web_state = config["ACTIONMAPPINGS"][all_files.index(actionfile)]["web"]
        timeout = config["ACTIONMAPPINGS"][all_files.index(actionfile)]["timeout"]
        memory = config["ACTIONMAPPINGS"][all_files.index(actionfile)]["memory"]

        os.system("cp {} __main__.py".format(actionfile))

        os.system("{}action delete {}".format(clistart, actionname))
        os.system("zip -r {}.zip virtualenv __main__.py osnapi.py secretmanager.py > /dev/null".format(actionname))
        os.system(
            "{}action create {} --kind python:3 {}.zip --timeout {} --memory {} --web {}".format(clistart,
                                                                                                 actionname,
                                                                                                 actionname,
                                                                                                 timeout,
                                                                                                 memory,
                                                                                                 web_state))
        os.system("rm {}.zip".format(actionname))

os.system("rm __main__.py")

if fresh_start:
    # meta sequence action
    os.system("{}action delete metasequenceaction".format(clistart))
    os.system("{}action create metasequenceaction --sequence getmetadataaction,handlemetadataaction".format(clistart))

    # value sequence action
    os.system("{}action delete valuesequenceaction".format(clistart))
    os.system("{}action create valuesequenceaction --sequence getcsvaction,handlecontentdataaction".format(clistart))

    # complete sequence action
    os.system("{}action delete completesequenceaction".format(clistart))
    os.system(
        "{}action create completesequenceaction --sequence metasequenceaction,valuesequenceaction".format(clistart))

for file in all_files:
    config["FILEHASHES"][file] = get_digest(file)

with open(config_path, 'w') as outfile:
    json.dump(config, outfile)

os.system("rm secretmanager.py")
