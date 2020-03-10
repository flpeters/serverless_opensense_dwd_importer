#!/usr/bin/python3

"""monitorapp.py: This program starts up a flask server, to provide an easier usage of other programs as autodeploy"""

__author__ = "Ahmet Kilic https://github.com/flamestro"

import datetime
import json
import os
import sys
from threading import Lock

from flask import Flask, request
from flask_cors import CORS

import deployment_tmp.config_template as conf_template

arguments = sys.argv[1:]
clistart = None
lock = Lock()


# Utils
def get_root_dir():
    root_dir = os.path.abspath(__file__)
    for _ in range(0):
        root_dir = os.path.dirname(root_dir)
    return root_dir


def substring_maker(inputstring, start, end, index=0):
    return (inputstring.split(start))[-1].split(end)[index]


def get_logs_json():
    root_dir = os.path.abspath(__file__)
    for _ in range(1):
        root_dir = os.path.dirname(root_dir)
    return root_dir + "/data/logs.json"


def load_config():
    root_dir = os.path.abspath(__file__)
    for _ in range(1):
        root_dir = os.path.dirname(root_dir)
    config_path = root_dir + "/data/config.json"
    if not os.path.isfile(config_path):
        with lock:
            with open(config_path, 'w+') as file:
                json_data = conf_template.__CONFIGTEMPLATE__
                json.dump(json.loads(json_data), file)
                print("Created config file under {} please enter informations to continue".format(config_path))
                sys.exit(0)
    with open(config_path, "r") as f:
        raw = f.read()
        config = json.loads(raw)
        return config


# verifier

def get_cli_start_param_by_deployment(deployment_param):
    if deployment_param == "IBM":
        clistartparam = "ibmcloud fn "
    elif deployment_param == "REMOTE":
        clistartparam = "wsk "
    else:
        clistartparam = "wsk -i "
    return clistartparam


def verify_deployment(deployment_param):
    if deployment_param == "IBM":
        result = "IBM"
    elif deployment_param == "REMOTE":
        result = "REMOTE "
    else:
        result = "LOCAL"
    return result


def verify_calls(calls):
    try:
        int(calls)
        return calls
    except:
        return "1"
    finally:
        return "1"


def verify_fresh(fresh):
    if fresh.lower() == "true":
        result = "true"
    else:
        result = "false"
    return result


# init app
def init_args():
    """
    initializes arguments, if called from console
    :return:
    """
    global clistart
    try:
        deployment_option_index = arguments.index("--deployment")
        clistart = get_cli_start_param_by_deployment(arguments[deployment_option_index + 1])
    except ValueError:
        clistart = "wsk -i "
    if not os.path.isfile(get_logs_json()):
        with lock:
            with open(get_logs_json(), 'w+') as file:
                json_data = '''{"actionCount": 0,"aimedValues": 0,"values": 0,"achieved": 0,"lostValues": 0,
                                "differenceInLastfivemins": 0,"newActions": 0,"lastTimeStamp": "0000-00-00 00:00:00.000000",
                                "latestSeenTimeStamps" : []}'''
                json.dump(json.loads(json_data), file)


init_args()

app = Flask(__name__)
CORS(app)


# routes

@app.route('/logs/')
def logs():
    """
    Creates logs by reading from the MongoDB and adding them into the logs.json
    :return: {log_state:<LOGGING|NOT_LOGGING>, latestLogs:[list of log objects for frontend]}
    """
    log_state = "LOGGING"
    with open(get_logs_json(), "r") as f:
        raw = f.read()
        logs = json.loads(raw)
    try:
        stats = os.popen(clistart + ' action invoke handleconfig --blocking --result --param printID 10').read()
        if stats == '':
            log_state = "NOT_LOGGING"
        datenew = str(datetime.datetime.now())
        logs[datenew] = json.loads(stats)
        logs["actionCount"] = logs[datenew]["message"][1]["actionCount"]
        logs["aimedValues"] = logs[datenew]["message"][0]["aimedValueCount"]
        logs["values"] = logs[datenew]["message"][0]["valueCount"]
        try:
            logs["achieved"] = logs[datenew]["message"][0]["valueCount"] / logs[datenew]["message"][0][
                "aimedValueCount"]
        except:
            pass
        logs["lostValues"] = logs[datenew]["message"][0]["aimedValueCount"] - logs[datenew]["message"][0]["valueCount"]
        try:
            logs["differenceInLastfivemins"] = logs[datenew]["message"][0]["valueCount"] - \
                                               logs[logs["lastTimeStamp"]]["message"][0]["valueCount"]
            logs["newActions"] = logs[datenew]["message"][1]["actionCount"] - logs[logs["lastTimeStamp"]]["message"][1][
                "actionCount"]
        except:
            pass
        logs["lastTimeStamp"] = str(datenew)
        if len(logs["latestSeenTimeStamps"]) < 10:
            logs["latestSeenTimeStamps"].append(logs["lastTimeStamp"])
        else:
            logs["latestSeenTimeStamps"] = logs["latestSeenTimeStamps"][1:10]
            logs["latestSeenTimeStamps"].append(logs["lastTimeStamp"])
    except Exception as e:
        pass
    with lock:
        with open(get_logs_json(), 'w') as outfile:
            json.dump(logs, outfile)
    result = {"logState": log_state, "latestLogs": []}
    for ts in logs["latestSeenTimeStamps"]:
        result["latestLogs"].append({"aimedValues": logs[ts]["message"][0]["aimedValueCount"],
                                     "reachedValues": logs[ts]["message"][0]["valueCount"],
                                     "actionCount": logs[ts]["message"][1]["actionCount"]})

    return result


@app.route('/deploy/')
def deployActions():
    """
    Deploys actions with an given deployment type
    :return:
    """
    with lock:
        deployment = verify_deployment(request.args.get('deployment'))
        fresh = verify_fresh(request.args.get('fresh'))
        os.popen('cd deployment_tmp && python autodeploy.py --deployment {} --fresh {}'.format(deployment, fresh)).read()
    return {"message": "DEPLOYED_ACTIONS"}


@app.route('/import/')
def importData():
    """
    Starts an import process with optional scale
    :param calls
    :return:
    """
    with lock:
        calls = verify_calls(request.args.get('calls'))
        result = os.popen(
            clistart + 'action invoke filenamesplitteraction --blocking --result --param calls {}'.format(calls)).read()
        action_expected = [int(s) for s in result.split() if s.isdigit()]
    return {"actionsExpected": action_expected[0]}


@app.route('/deleteActions/')
def deleteActions():
    """
    Deletes all actions which are mentioned in the config
    :return:
    """
    with lock:
        deployment = request.args.get('deployment')
        delete_return = os.popen('cd deployment_tmp && python deleteactions.py --deployment {} '.format(deployment)).read()

    return {"message": delete_return}


@app.route('/getActions/')
def getActions():
    """
    builds <li> tags for the frontend including information about hosted actions.
    :return:
    """
    delete_return = os.popen(clistart + 'list').read()
    result = substring_maker(delete_return, "actions", "triggers", 0)
    subs = str(result).split("\n")
    list_elems = ""
    for x in subs[1:]:
        list_elems += '<li class="list-group-item">' + x + " </li>"
    return {"message": list_elems}


@app.route('/clearLogs/')
def clearLogs():
    """
    clears logs in logs.json
    :return:
    """
    with lock:
        with open(get_logs_json(), 'w+') as file:
            json_data = '''{"actionCount": 0,"aimedValues": 0,"values": 0,"achieved": 0,"lostValues": 0,
                            "differenceInLastfivemins": 0,"newActions": 0,"lastTimeStamp": "0000-00-00 00:00:00.000000",
                            "latestSeenTimeStamps" : []}'''
            json.dump(json.loads(json_data), file)
    return {"message": "dones"}


@app.route('/clearMongo/')
def clearMongoDB():
    """
    clears all sensor mappings in MongoDB
    :return:
    """
    with lock:
        stats = os.popen(clistart + ' action invoke handleconfig --blocking --result --param rewrite yes').read()

    return {"message": stats}


@app.route('/isImporting/')
def isImporting():
    """
    approximates if an import process is running by checking if any of the functions in the config where started in
    the last 7 minutes.
    :return: returns one of two states <"IMPORT"|"NO_IMPORT">
    """
    deployment = verify_deployment(request.args.get('deployment'))
    delete_return = os.popen(clistart + 'activation list').read()
    subs = str(delete_return).split("\n")
    result = ""
    if deployment == "IBM" or deployment == "LOCAL" or deployment == "REMOTE":
        all_actions = [obj["actionname"] for obj in load_config()["ACTIONMAPPINGS"]]
        all_actions.remove("handleconfig")
    for x in subs[1:]:
        try:
            line_vals = x.split(" ")
            line_vals.remove("")
            line_vals = [i for i in line_vals if not i == ""]
            if len(line_vals) > 7:
                if any(x in line_vals[7] for x in all_actions):
                    daydif = datetime.datetime.now().day - int(line_vals[0].split("-")[2])
                    hourdif = datetime.datetime.now().hour - int(line_vals[1].split(":")[0])
                    mindif = datetime.datetime.now().minute - int(line_vals[1].split(":")[1])
                    if daydif > 0 or hourdif > 1 or (mindif > 7 or mindif < 0):
                        result = "NO_IMPORT"
                        break
                    else:
                        result = "IMPORT"
                        break
                else:
                    continue
        except Exception as e:
            print("isImporting ", e)
        result = "NO_IMPORT"
    return {"message": result}


if __name__ == '__main__':
    load_config()
    app.run()
