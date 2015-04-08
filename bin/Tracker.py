"""
Tracks the status of apps. Designed to be run on a cron, but can be run manually for debugging purposes.
"""

import os
import sys
import logging
from collections import defaultdict

# Add relative path libraries
SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.append(os.path.abspath(os.path.sep.join([SCRIPT_DIR, "..", "lib"])))

import Repository
import AppServices
import SampleServices
import ConfigurationServices

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
        logfile = ConfigurationServices.GetConfig("TRACKER_LOG_FILE")
        if not os.access(os.path.dirname(logfile), os.W_OK):
            print "log directory: %s does not exist or is not writeable" % (logfile)
            sys.exit(1)
        logging.basicConfig(filename=logfile, level=args.loglevel, format=ConfigurationServices.GetConfig("LogFormat"))
    #logging.basicConfig(level=args.loglevel)

    pl = logging.getLogger("peewee")
    pl.setLevel(logging.INFO)

    logging.debug("Starting tracker")

    if args.id:
        sampleApps = [ Repository.GetSampleAppByID(opts.id) ]
    else:
        # get all the SampleApps with statuses that the Tracker will be able to update
        # these represent "live" statuses on BaseSpace
        constraints = { "status" : [ "submitted", "pending", "running" ] }
        sampleApps = Repository.GetSampleAppByConstraints(constraints)
        logging.debug("Working on %i samples" % len(sampleApps))

    # there's quite a lot code shared here with QCChecker.py, to iterate over SampleApps and update them

    # record what transitions we make (state -> state for each SampleApp) so we can report at the end
    # all SampleApps will end up in either "qc-failed" or "qc-passed" states
    transitions = defaultdict(list)
    for sampleApp in sampleApps:
        # unpack the SampleApp a little
        sampleName = Repository.SampleAppToSampleName(sampleApp)
        appName = Repository.SampleAppToAppName(sampleApp)
        sampleAppId = Repository.SampleAppToBaseSpaceId(sampleApp)
        logging.debug("working on: %s %s" % (sampleName, appName))

        if not sampleAppId:
            logging.warn("No BaseSpace Id for SampleApp: %s" % Repository.SampleAppSummary(sampleApp))
            continue
        # get the new status
        newstatus = AppServices.GetAppStatus(sampleAppId)
        if args.safe:
            logging.info("would update %s to: %s" % (Repository.SampleAppSummary(sampleApp), newstatus))
        else:
            # record the transition and update in the db
            transition = (Repository.SampleAppToStatus(sampleApp), newstatus)
            Repository.SetSampleAppStatus(sampleApp, newstatus)
            transitions[transition].append(sampleAppId)

    # log how many of each transition we've made. If the number is low enough, report which apps have had each transition type
    for transition in sorted(transitions):
        if len(transitions[transition]) > 40:
            logging.info("%s : %i" % (transition, len(transitions[transition])))
        else:
            logging.info(
                "%s : %i (%s)" % (
                    transition, len(transitions[transition]), ", ".join([str(x) for x in transitions[transition]])))
    logging.debug("Finished tracker")

