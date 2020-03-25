# Serverless migration of Opensense.network's DWD agent

The current implementation of [Opensense.network's](https://www.opensense.network/) "Deutscher Wetterdienst" data importer agent is a monolith and therefore has some limitations.  
With our approach, we try a new, serverless solution.  

> By [Ahmet Kilic](https://github.com/flamestro) and [Florian Peters](https://github.com/flpeters)

## Why Serverless? 
- You only pay for your actual computations
- Architecture inherent scalability
- Fully managed application deployment

## OpenWhisk
- For our implementation we use [Apache OpenWhisk](https://openwhisk.apache.org/) as our FaaS provider.
- After installation of OpenWhisk, actions can be created with:`wsk action create someName --kind python:3 --main functionName fileName.py`
- The actions can then be invoked with : `wsk action invoke /<namespace>/someName --blocking --result`
- They can also be deleted with : `wsk action delete /<namespace>/someName`
- To get some logs for your last invocation just do : `wsk actiovation logs -l`
- To list available actions do : `wsk list`
- You can also get some information about your actions by doing : `wsk action get <action-name>`

## Setup
All of your personal data, including passwords and file paths, need to be placed inside a `config.json` file in the `./data` directory.  
This file will be automatically created from a template when you first run `./monitorapp.py` (see below for more info), and wont be included when using git for version control.

### requirements
- [docker](https://www.docker.com/)
- virtualenv python
- [wsk cli](https://github.com/apache/openwhisk-cli)

### Apps
The following apps are used internally to create, delete, copy and paste files in your system (including manipulation of .wskprops).  
The apps themselves use the command line and rely on `wsk` being installed.  

### `./monitorapp.py`
This app starts a local flask server, which hosts a web-interface, allowing you to control deployment, deletion, imports, logging, monitoring, and clearing logs without having to type any commands yourself.  

Just run `python monitorapp.py` and open `./web/index.html` in a browser.

For this to work correctly, fill out `WSKPROPSPATH` in the `./data/config.json` file, which is created after executing this app the first time. The path should point to the folder with your `.wskprops` files (`.wskpropsIBM`, `.wskpropsREMOTE`, `.wskpropsLOCAL`). You will have to at least enter the information for the platform you want to use (IBM, LOCAL, REMOTE).

If you deploy your functions for the first time over this app, check fresh deployment to create a virtualenv.

At the moment, the logs can be manipulated by refreshing the page.

### `./deployment_tmp/autodeploy.py`
This component will deploy your actions to openwhisk and set the credentials, specified in your config.json file. 

It will skip unchanged actions, so you can also use this to update actions.

Run `python deployment_tmp/autodeploy.py` to deploy all actions from config

- `--config <PATH_TO_CONFIG>` to use specific config / create config if not created

- `--deployment <IBM|LOCAL|REMOTE>` to adapt deployment request (LOCAL will mean -i)

- `--fresh <true|false>` to deploy every function from scratch without skipping unchanged functions

### `./deployment_tmp/deleteactions.py`
Running this will delete all actions specified in your config.json file.

Run `python deployment_tmp/deleteactions.py`

- `--config <PATH_TO_CONFIG>` to use specific config / create config if not created

- `--deployment <IBM|LOCAL|REMOTE>` to adapt deployment request (LOCAL will mean -i)

### `./deployment_tmp/wsksetup.py` and `./deployment_tmp/wskshutdown.py`
These components start and stop your local openwhisk environment.

Run `python deployment_tmp/wsksetup.py --config <PATH_TO_YOUR_CONFIG>` to start a local openwhisk env with your config.

If you dont have a config, enter any path to a config.json file and a config with all needed key value pairs will be created.

These scripts can be skipped if you don't want to deploy locally.

Run `python deployment_tmp/wskshutdown.py` to shutdown openwhisk docker containers.
