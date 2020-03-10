# Serverless migration of Opensense.network's DWD agent

The current implementation of [Opensense.network's](https://www.opensense.network/) "Deutscher Wetterdienst" agent is a monolith and therefore has some boundaries. 
With our approach, we want to try a new serverless solution.

## Why? 
- You only pay for your actual computations
- Automatic scalability
- Fully managed application deployment

## OpenWhisk
- For our implementation we will use Apache OpenWhisk as our FaaS provider.
- After installation of OpenWhisk, actions can be created with:`wsk action create someName --kind python:3 --main functionName fileName.py`
- The actions can then be invoked with : `wsk action invoke /<namespace>/someName --blocking --result`
- They can also be deleted with : `wsk action delete /<namespace>/someName`
- To get some logs for your last invocation just do : `wsk actiovation logs -l`
- To list available actions do : `wsk list`
- You can also get some information about your actions by doing : `wsk action get <action-name>`

## Setup

### requirements
- docker
- virtualenv python
- wsk cli

### Introduction
The following apps will create, delete, copy and paste files in your system (including manipulation of .wskprops).

They will also contain console calls!

### monitorapp.py
This app allows you to control deployment, deletion, imports, logging, monitoring and clearing logs in one place without cli.

It will need a WSKPROPSPATH in your config, which points to an folder with .wskprops files (.wskpropsIBM, .wskpropsREMOTE, .wskpropsLOCAL)

Just run `python monitorapp.py` and open `web/index.html` in a browser

If you deploy your functions for the first time over this app, then check fresh deployment to create a virtualenv.

Starting this app will create a config.json in the `data/` dir of this project. 
To use this app you will have to enter at least information for the platform you want to use (IBM, LOCAL, REMOTE)

The logs can be manipulated by refreshing the page at the moment

### autodeploy.py
This component will deploy your actions and set the credentials. 

It will skip not changed actions, so you can also use this to update actions.

Run `python deployment_tmp/autodeploy.py` to deploy all actions from config

- `--config <PATH_TO_CONFIG>` to use specific config / create config if not created

- `--deployment <IBM|LOCAL|REMOTE>` to adapt deployment request (LOCAL will mean -i)

- `--fresh <true|false>` to deploy every function from scratch without skipping not changed functions

### deleteactions.py
Running this will delete all actions in your config

Run `python deployment_tmp/deleteactions.py`

- `--config <PATH_TO_CONFIG>` to use specific config / create config if not created

- `--deployment <IBM|LOCAL|REMOTE>` to adapt deployment request (LOCAL will mean -i)

### wsksetup.py and wskshutdown.py
This components start and stop your local openwhisk env

Run `python deployment_tmp/wsksetup.py --config <PATH_TO_YOUR_CONFIG>` to start a local openwhisk env with your config

If you dont have an config enter any path to an config.json file and a config with all needed key value pairs will be created

This scripts can be skipped if you don't want to deploy locally

Run `python deployment_tmp/wskshutdown.py` to shutdown openwhisk docker containers
