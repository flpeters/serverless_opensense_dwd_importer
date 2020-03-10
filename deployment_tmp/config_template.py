"""
config_template.py: This file is an single point of contact for the scripts, which create empty config files
"""

__author__ = "Ahmet Kilic https://github.com/flamestro"

__CONFIGTEMPLATE__ = '''{"ACTIONMAPPINGS": [
                                {
                                  "filename": "src/placeholder_action/hello_placeholder_action.py",
                                  "actionname": "placeholderaction",
                                  "memory": 128,
                                  "timeout": 300000,
                                  "web": false
                                },
                                {
                                  "filename": "src/value_handling/get_ftp_filenames_action.py",
                                  "actionname": "getfilenamesaction",
                                  "memory": 128,
                                  "timeout": 300000,
                                  "web": true
                                },
                                {
                                  "filename": "src/value_handling/handle_content_data_action.py",
                                  "actionname": "handlecontentdataaction",
                                  "memory": 128,
                                  "timeout": 300000,
                                  "web": false
                                },
                                {
                                  "filename": "src/value_handling/get_csv_action.py",
                                  "actionname": "getcsvaction",
                                  "memory": 128,
                                  "timeout": 300000,
                                  "web": true
                                },
                                {
                                  "filename": "src/sensor_handling/handle_meta_data_action.py",
                                  "actionname": "handlemetadataaction",
                                  "memory": 128,
                                  "timeout": 300000,
                                  "web": true
                                },
                                {
                                  "filename": "src/sensor_handling/get_meta_data_action.py",
                                  "actionname": "getmetadataaction",
                                  "memory": 128,
                                  "timeout": 300000,
                                  "web": true
                                },
                                {
                                  "filename": "src/filenamesplitter_action.py",
                                  "actionname": "filenamesplitteraction",
                                  "memory": 128,
                                  "timeout": 300000,
                                  "web": true
                                },
                                {
                                  "filename": "src/handle_config.py",
                                  "actionname": "handleconfig",
                                  "memory": 128,
                                  "timeout": 300000,
                                  "web": true
                                }
                              ],
                              "WSKPROPSPATH": "",
                              "FILEHASHES": {},
                              "MONGOURLIBM": "",
                              "MONGOURLLOCAL": "",
                              "DEVTOOLS": "",
                              "OSNUSERNAME": "",
                              "OSNPASSWORD": "",
                              "MONGOURLREMOTE": "",
                              "OPENWHISKUSERNAMEREMOTE": "",
                              "OPENWHISKPWDREMOTE": "",
                              "OPENWHISKUSERNAMELOCAL": "",
                              "OPENWHISKPWDLOCAL": "",
                              "OPENWHISKUSERNAMEIBM": "",
                              "OPENWHISKPWDIBM": ""
                            }'''