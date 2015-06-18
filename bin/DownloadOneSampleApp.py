"""
Tool to download the appropriate files for a finished app. 

In normal operation, will be called as a subprocess from Downloader.py but can also be run manually.
"""

import os
import sys
import logging

# Add relative path libraries
SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.append(os.path.abspath(os.path.sep.join([SCRIPT_DIR, "..", "lib"])))
sys.path.append(os.path.abspath(os.path.sep.join([SCRIPT_DIR, "..", "..", "basespace-python-sdk", "src"])))

from BaseSpacePy.api.BaseSpaceAPI import BaseSpaceAPI

from ConfigurationServices import ConfigurationServices
from DataAccessRead import DataAccessRead
from SampleServices import SampleServices
from AppServices import AppServices


class DownloadException(Exception):
    pass


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-i', '--id', type=str, dest="id", required=True, help='local ID of SampleApp')
    parser.add_argument('-l', '--logfile', type=str, dest="logfile", default="", help='path to logfile')

    parser.add_argument("-L", "--loglevel", dest="loglevel", default="INFO",
                        help="loglevel, default INFO. Choose from WARNING, INFO, DEBUG")
    args = parser.parse_args()

    baseSpaceAPI = BaseSpaceAPI()
    configuration_services = ConfigurationServices()
    db_config = configuration_services.get_config("DBFile")
    data_access_read = DataAccessRead(db_config, configuration_services)
    sample_services = SampleServices(baseSpaceAPI, configuration_services)
    app_services = AppServices(baseSpaceAPI, sample_services, configuration_services, data_access_read)

    if args.logfile:
        logging.basicConfig(filename=args.logfile, level=args.loglevel,
                            format=ConfigurationServices.GetConfig("LogFormat"))
    else:
        logging.basicConfig(level=args.loglevel, format=ConfigurationServices.GetConfig("LogFormat"))

    sample_app = data_access_read.get_proto_apps_by_constraints({"id": args.id})

    try:
        logging.debug("Downloading SampleApp: %s" % sample_app)
        attempt = 0
        MAX_ATTEMPTS = configuration_services.get_config("MAX_ATTEMPTS")
        app_services.download_deliverable(sample_app)
        sample_app.set_status("downloaded")
    except Exception as e:
        sample_app.set_status("download-failed", str(e))
        logging.error(str(e))