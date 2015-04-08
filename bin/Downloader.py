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

import Repository
import ConfigurationServices

class DownloaderException(Exception):
    pass

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='update status of sample/apps')
    parser.add_argument('-i', '--id', type=str, dest="id", help='update just a specific SampleApp id')
    parser.add_argument('-s', '--safe', dest="safe", default=False, action="store_true", help='safe mode - say what you would do without doing it')
    parser.add_argument('-l', '--logtostdout', dest="logtostdout", default=False, action="store_true", help="log to stdout instead of default log file")
    parser.add_argument("-L", "--loglevel", dest="loglevel", default="INFO", help="loglevel, default INFO. Choose from WARNING, INFO, DEBUG")
    args = parser.parse_args()

    if args.safe or args.logtostdout:
        logging.basicConfig(level=args.loglevel, format=ConfigurationServices.GetConfig("LogFormat"))
    else:
        logfile = ConfigurationServices.GetConfig("DOWNLOADER_LOG_FILE")
        if not os.access(os.path.dirname(logfile), os.W_OK):
            print "log directory: %s does not exist or is not writeable" % (logfile)
            sys.exit(1)
        logging.basicConfig(filename=logfile, level=args.loglevel, format=ConfigurationServices.GetConfig("LogFormat"))

    pl = logging.getLogger("peewee")
    pl.setLevel(logging.INFO)

    logging.debug("Starting downloader")

    if args.id:
        sampleApps = [ Repository.GetSampleAppByID(args.id) ]
    else:
        constraints = { "status" : [ "qc-passed" ] }
        sampleApps = Repository.GetSampleAppByConstraints(constraints)
        logging.debug("Working on %i samples" % len(sampleApps))

    if not sampleApps:
        # nothing to do
        sys.exit(0)

    # get the apps that are already downloading, so we can count them and make sure we don't have too many
    constraints = { "status" : [ "downloading" ]}
    downloadingSampleApps = Repository.GetSampleAppByConstraints(constraints)
    numRunningDownloads = len(downloadingSampleApps)
    MAX_DOWNLOADS = ConfigurationServices.GetConfig("MAX_DOWNLOADS")
    logging.info("There are currently %d downloads running (%d maximum allowed)" % (numRunningDownloads, MAX_DOWNLOADS))

    # compute the number of downloads we could kick off this round
    numberToDownload = MAX_DOWNLOADS - numRunningDownloads
    numberSetToDownload = 0

    # we'll use the PYTHON_EXE to invoke the download commands
    PYTHON_EXE = ConfigurationServices.GetConfig("PYTHON_EXE")

    while numberSetToDownload < numberToDownload:
        try:
            sampleApp = sampleApps[numberSetToDownload]
        except IndexError:
            # this will fire when we run out of app results to download
            break
        # build up the downlaod command
        downloadScript = os.path.join(SCRIPT_DIR, "DownloadOneSampleApp.py")
        outputDir = Repository.SampleAppToOutputDirectory(sampleApp)
        logDirName = ConfigurationServices.GetConfig("SAMPLE_LOG_DIR_NAME")
        logfile = os.path.join(outputDir, logDirName, "download.log")

        #cmd = "%s %s -i %s" % (PYTHON_EXE, downloadScript, Repository.SampleAppToId(sampleApp))
        cmd = [ PYTHON_EXE, downloadScript, "-i", str(Repository.SampleAppToId(sampleApp)), "-l", logfile ]
        if args.safe:
            logging.debug("would download: %s" % Repository.SampleAppSummary(sampleApp))
            logging.debug("would use command: %s" % " ".join(cmd))
            numberSetToDownload += 1 
        else: 
            # launch the command and set the status based on whether the launch was successful
            logging.info("executing command: %s" % " ".join(cmd))
            try:
                process = subprocess.Popen(cmd)
                pid = process.pid
            except Exception as e:
                Repository.SetSampleAppStatus(sampleApp, "download-failed", "%s : %s" % (cmd, str(e)))
                raise DownloaderException("Downloader failed to launch command: %s (%s)" % (cmd, str(e)))
            logging.info("launched download process: %s" % pid)
            Repository.SetSampleAppStatus(sampleApp, "downloading", "pid: %s" % pid)
            numberSetToDownload += 1


