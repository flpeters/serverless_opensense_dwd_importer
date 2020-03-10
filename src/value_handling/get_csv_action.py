"""get_csv_action.py: This action loads the value data of sensors and then returns it as a string"""

__author__ = "Ahmet Kilic https://github.com/flamestro"

import io
from urllib.request import urlopen
from zipfile import ZipFile

try:
    import secretmanager
except:
    import deployment_tmp.secret_manager as secretmanager


def main(args):
    file_name = args.get("filename")
    rest_names = args.get("restfilenames")
    if file_name is None:
        return {"error": "seuquence should be stopped"}
    try:
        ftp_url = "ftp://ftp-cdc.dwd.de/" + args.get("ftp_url",
                                                     "climate_environment/CDC/observations_germany/climate/hourly/air_temperature/recent/")
        inner_file_name = "COULD NOT GET FILENAME"
        sensorzip = urlopen(ftp_url + file_name)
        memfile = io.BytesIO(sensorzip.read())

        with ZipFile(memfile, 'r') as myzip:
            try:
                for z_info in myzip.filelist:
                    if z_info.filename.startswith("produkt"):
                        inner_file_name = z_info.filename
            except Exception as e:
                print(e)
            finally:
                csv_file_value_data = myzip.open(inner_file_name)
        result = {"csv": str(csv_file_value_data.read())[2:-1],
                  "restfilenames": rest_names}
        print("send in get csv")
        return result
    except Exception as e:
        secretmanager.complete_sequence(rest_names)
        result = {"error": "failed metadata because of unkown error - jump to next file"}
        print(result, e)
        return result
