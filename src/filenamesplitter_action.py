"""filenamesplitter_action.py: This action is the entry point to the DWD import process"""

__author__ = "Ahmet Kilic https://github.com/flamestro"

import sys
import warnings

try:
    import secretmanager
except:
    import deployment_tmp.secret_manager as secretmanager


def split_list(alist, wanted_parts=1):
    length = len(alist)
    return [alist[i * length // wanted_parts: (i + 1) * length // wanted_parts]
            for i in range(wanted_parts)]


if not sys.warnoptions:
    warnings.simplefilter("ignore")


def main(args):
    pipeline_calls = args.get("calls", 1)
    try:
        namelist = secretmanager.get_filename_list_action()
        print("namelist len unsplitted (should be 1) {}".format(len(namelist)))
    except Exception as e:
        print(e)
        return {"message": "fail in namelist"}
    name_list_array = namelist["filenames"].split(",")
    zip_list_array = [x for x in name_list_array if x.endswith(".zip") and x is not None]

    print("Init call Complete Data ", len(zip_list_array))
    if pipeline_calls < 1:
        return {"message": "troll someone else"}
    if pipeline_calls == 1:
        zip_list_array.append("end")
        zip_list_array.append("end")
        if len(zip_list_array) > 1:
            try:
                response = secretmanager.complete_sequence(zip_list_array)
                print(response)
            except Exception as e:
                print("send handle completedata events to URLAPIcompletesequenceaction", e)
                pass
    elif pipeline_calls > 1:
        for x in split_list(zip_list_array, pipeline_calls):
            x.append("end")
            x.append("end")
            if len(x) > 1:
                try:
                    response = secretmanager.complete_sequence(x)
                    print(response)
                except Exception as e:
                    print("send handle completedata events to URLAPIcompletesequenceaction", e)
                    pass
    else:
        return {"message": "invalid param"}

    return {"message": "tried to start file proccesses : " + str(len(zip_list_array)) + " "}
