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

import Repository
import AppServices
import ConfigurationServices

class DownloadException(Exception):
    pass

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-i', '--id', type=str, dest="id", required=True, help='local ID of SampleApp')
    parser.add_argument('-l', '--logfile', type=str, dest="logfile", default="", help='path to logfile')

    parser.add_argument("-L", "--loglevel", dest="loglevel", default="INFO", help="loglevel, default INFO. Choose from WARNING, INFO, DEBUG")
    args = parser.parse_args()

    if args.logfile:
        logging.basicConfig(filename=args.logfile, level=args.loglevel, format=ConfigurationServices.GetConfig("LogFormat"))
    else:
        logging.basicConfig(level=args.loglevel, format=ConfigurationServices.GetConfig("LogFormat"))

    sampleApp = Repository.GetSampleAppByID(args.id)
    try:
        logging.debug("Downloading SampleApp: %s %s" % (Repository.SampleAppToSampleName(sampleApp), Repository.SampleAppToAppName(sampleApp)))
        attempt = 0
        MAX_ATTEMPTS = ConfigurationServices.GetConfig("MAX_ATTEMPTS")
        AppServices.download_deliverable(sampleApp)
        Repository.SetSampleAppStatus(sampleApp, "downloaded")
    except Exception as e:
        Repository.SetSampleAppStatus(sampleApp, "download-failed", str(e))
        logging.error(str(e))