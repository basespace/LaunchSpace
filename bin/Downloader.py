"""
For samples that are qc-passed, initiate a download of the appropriate files.

Designed to be run on cron but can be run manually for debugging purposes
"""

import os
import sys
import logging
import subprocess

# Add relative path libraries
SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.append(os.path.abspath(os.path.sep.join([SCRIPT_DIR, "..", "lib"])))
sys.path.append(os.path.abspath(os.path.sep.join([SCRIPT_DIR, "..", "..", "basespace-python-sdk", "src"])))

from BaseSpacePy.api.BaseSpaceAPI import BaseSpaceAPI

from ConfigurationServices import ConfigurationServices
from DataAccessRead import DataAccessRead
from SampleServices import SampleServices
from AppServices import AppServices


class DownloaderException(Exception):
    pass


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='update status of sample/apps')
    parser.add_argument('-i', '--id', type=str, dest="id", help='update just a specific SampleApp id')
    parser.add_argument('-s', '--safe', dest="safe", default=False, action="store_true",
                        help='safe mode - say what you would do without doing it')
    parser.add_argument('-l', '--logtostdout', dest="logtostdout", default=False, action="store_true",
                        help="log to stdout instead of default log file")
    parser.add_argument("-L", "--loglevel", dest="loglevel", default="INFO",
                        help="loglevel, default INFO. Choose from WARNING, INFO, DEBUG")
    args = parser.parse_args()

    baseSpaceAPI = BaseSpaceAPI()
    configuration_services = ConfigurationServices()
    db_config = configuration_services.get_config("DBFile")
    data_access_read = DataAccessRead(db_config, configuration_services)
    sample_services = SampleServices(baseSpaceAPI, configuration_services)
    app_services = AppServices(baseSpaceAPI, sample_services, configuration_services, data_access_read)

    if args.safe or args.logtostdout:
        logging.basicConfig(level=args.loglevel, format=configuration_services.get_config("LogFormat"))
    else:
        logfile = configuration_services.get_config("DOWNLOADER_LOG_FILE")
        if not os.access(os.path.dirname(logfile), os.W_OK):
            print "log directory: %s does not exist or is not writeable" % (logfile)
            sys.exit(1)
        logging.basicConfig(filename=logfile, level=args.loglevel,
                            format=configuration_services.get_config("LogFormat"))

    pl = logging.getLogger("peewee")
    pl.setLevel(logging.INFO)

    logging.debug("Starting downloader")

    if args.id:
        sample_apps = [data_access_read.get_sample_app_by_id(args.id)]
    else:
        constraints = {"status": ["qc-passed"]}
        sample_apps = data_access_read.get_sample_apps_by_constraints(constraints)
        logging.debug("Working on %i samples" % len(sample_apps))

    if not sample_apps:
        # nothing to do
        sys.exit(0)

    # get the apps that are already downloading, so we can count them and make sure we don't have too many
    constraints = {"status": ["downloading"]}
    downloading_sampleapps = data_access_read.get_sample_apps_by_constraints(constraints)
    num_running_downloads = len(downloading_sampleapps)
    MAX_DOWNLOADS = configuration_services.get_config("MAX_DOWNLOADS")
    logging.info("There are currently %d downloads running (%d maximum allowed)" % (num_running_downloads, MAX_DOWNLOADS))

    # compute the number of downloads we could kick off this round
    numberToDownload = MAX_DOWNLOADS - num_running_downloads
    number_set_to_download = 0

    # we'll use the PYTHON_EXE to invoke the download commands
    PYTHON_EXE = configuration_services.get_config("PYTHON_EXE")

    while number_set_to_download < numberToDownload:
        try:
            sample_app = sample_apps[number_set_to_download]
        except IndexError:
            # this will fire when we run out of app results to download
            break
        # build up the download command
        download_script = os.path.join(SCRIPT_DIR, "DownloadOneSampleApp.py")
        output_dir = sample_app.get_path()
        log_dir_name = configuration_services.get_config("SAMPLE_LOG_DIR_NAME")
        logfile = os.path.join(output_dir, log_dir_name, "download.log")

        #cmd = "%s %s -i %s" % (PYTHON_EXE, downloadScript, Repository.SampleAppToId(sampleApp))
        cmd = [PYTHON_EXE, download_script, "-i", str(sample_app.id), "-l", logfile]
        if args.safe:
            logging.debug("would download: %s" % sample_app)
            logging.debug("would use command: %s" % " ".join(cmd))
            number_set_to_download += 1
        else:
            # launch the command and set the status based on whether the launch was successful
            logging.info("executing command: %s" % " ".join(cmd))
            try:
                process = subprocess.Popen(cmd)
                pid = process.pid
            except Exception as e:
                e_msg = "%s : %s" % (cmd, str(e))
                sample_app.set_status("download-failed", e_msg)
                raise DownloaderException("Downloader failed to launch command: %s" % e_msg)
            logging.info("launched download process: %s" % pid)
            sample_app.set_status("downloading", "pid: %s" % pid)
            number_set_to_download += 1


