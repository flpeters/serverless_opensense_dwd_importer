"""get_meta_data_action.py: This action loads the meta data of sensors and then returns it as a string"""

__author__ = "Ahmet Kilic https://github.com/flamestro"

import io
from urllib.request import urlopen
from zipfile import ZipFile
try:
    import secretmanager
except:
    import deployment_tmp.secret_manager as secretmanager

def main(args):
    inner_file_name = "COULD NOT GET FILENAME"
    file_name = args.get("filename")
    rest_names = args.get("restfilenames")
    try:
        ftp_url = "ftp://ftp-cdc.dwd.de/" + args.get("ftp_url",
                                                     "climate_environment/CDC/observations_germany/climate/hourly/air_temperature/recent/")

        sensorzip = urlopen(ftp_url + file_name)
        memfile = io.BytesIO(sensorzip.read())

        with ZipFile(memfile, 'r') as myzip:
            try:
                for z_info in myzip.filelist:
                    substrings = z_info.filename.split("_")
                    if substrings[0] == "Stationsmetadaten" or (
                            substrings[0] == "Metadaten" and substrings[1] == "Geographie"):
                        inner_file_name = z_info.filename
            except Exception as e:
                print(e)
            finally:
                meta_data = myzip.open(inner_file_name)
        result = {"metadata": str(meta_data.read())[2:-1],
                  "filename": file_name,
                  "restfilenames": rest_names}
        print("send in get metadata", result)
        return result
    except Exception as e:
        secretmanager.complete_sequence(rest_names)
        result = {"message": "failed metadata because of unkown error - jump to next file"}
        return result
