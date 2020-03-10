"""
dwd_agent.py: This program should simulate the import process locally (for debugging purposes)
"""

__author__ = "Ahmet Kilic https://github.com/flamestro"

import sys
import warnings

from src.sensor_handling.get_meta_data_action import main as get_meta_data
from src.sensor_handling.handle_meta_data_action import main as handle_meta_data
from src.value_handling.get_csv_action import main as get_csv_data
from src.value_handling.get_ftp_filenames_action import main as get_file_names
from src.value_handling.handle_content_data_action import main as handle_content_data


# TODO: Implement MOCK OSNAPI and MOCK secretmanager to run functions locally and test them

def split_list(alist, wanted_parts=1):
    length = len(alist)
    return [alist[i * length // wanted_parts: (i + 1) * length // wanted_parts]
            for i in range(wanted_parts)]


if not sys.warnoptions:
    warnings.simplefilter("ignore")


def main():
    try:
        namelist = get_file_names({"path": "climate_environment/CDC/observations_germany/climate/hourly/air_temperature/recent/"})
        print(namelist)
    except Exception as e:
        print(e)
        return {"message": "fail in namelist"}
    name_list_array = namelist["filenames"].split(",")
    for name in name_list_array[10:12]:
        if name.endswith(".zip"):
            print("Handle Complete Data from", name)
            try:
                metadata = get_meta_data({"filename": name})
            except:
                print("error in metadata")
            try:
                handlemetadata = handle_meta_data(metadata)
            except:
                print("error in handlemetadata")
            try:
                csv = get_csv_data(handlemetadata)
            except:
                print("error in csv")
            try:
                handle_content_data(csv)
            except Exception as e:
                print("Error in dwd content", e)
                pass
            print(name)

    return {"message": "tried to start all file proccesses :"+str(len(name_list_array))}


main()
