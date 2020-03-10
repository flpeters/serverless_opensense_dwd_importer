"""
get_ftp_filenames_action.py: This action loads the filenames inside an FTP dir, to scale the import process by names
"""

__author__ = "Ahmet Kilic https://github.com/flamestro"

from ftplib import FTP


# GET RELEVANT ZIP FILE NAMES
def main(args):
    path = args.get("path", "climate_environment/CDC/observations_germany/climate/hourly/air_temperature/recent/")
    ftp = FTP('ftp-cdc.dwd.de')
    ftp.login()
    files = ftp.nlst(path)
    filenames = ""

    for f in files:
        name = f.split('/')[-1]
        if filenames != "":
            filenames += "," + name
        else:
            filenames += name
    ftp.quit()
    return {"filenames": filenames}
