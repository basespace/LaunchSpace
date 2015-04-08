"""
Tool that launches apps that meet the launch conditions.

Designed to be run on the cron but can also be run manually for debugging purposes.
"""
import os
import sys
import logging
import datetime

# Add relative path libraries
SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.append(os.path.abspath(os.path.sep.join([SCRIPT_DIR, "..", "lib"])))

import Repository
import AppServices
import SampleServices
import ConfigurationServices

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Launch sample apps when ready')
    parser.add_argument('-i', '--id', type=str, dest="id", help='attempt to launch just a specific SampleApp id')
    parser.add_argument('-s', '--safe', dest="safe", default=False, action="store_true", help='safe mode - say what you would do without doing it')
    parser.add_argument('-Y', '--ignoreyield', dest="ignoreyield", default=False, action="store_true", help="ignore any missing yield")
    parser.add_argument('-l', '--logtostdout', dest="logtostdout", default=False, action="store_true", help="log to stdout instead of default log file")
    parser.add_argument("-L", "--loglevel", dest="loglevel", default="INFO", help="loglevel, default INFO. Choose from WARNING, INFO, DEBUG")
    args = parser.parse_args()

    if args.safe or args.logtostdout:
        logging.basicConfig(level=args.loglevel, format=ConfigurationServices.GetConfig("LogFormat"))
    else:
        logfile = ConfigurationServices.GetConfig("LAUNCHER_LOG_FILE")
        if not os.access(os.path.dirname(logfile), os.W_OK):
            print "log directory: %s does not exist or is not writeable" % (logfile)
            sys.exit(1)
        logging.basicConfig(filename=logfile, level=args.loglevel, format=ConfigurationServices.GetConfig("LogFormat"))

    #logging.basicConfig(level=args.loglevel)
    pl = logging.getLogger("peewee")
    pl.setLevel(logging.INFO)

    logging.debug("Starting launcher")

    if args.id:
        sampleApps = [ Repository.GetSampleAppByID(args.id) ]
    else:
        # get all the SampleApps with the waiting status
        constraints = { "status" : "waiting" }
        logging.debug("Finding samples")
        sampleApps = Repository.GetSampleAppByConstraints(constraints)
        logging.debug("working on %d samples" % len(sampleApps))

    for sampleApp in sampleApps:
        # unpack the SampleApp a little
        sampleName = Repository.SampleAppToSampleName(sampleApp)
        appName = Repository.SampleAppToAppName(sampleApp)
        # check whether the SampleApp is ready to launch, including getting a reason if it isn't ready
        ready, reason = AppServices.CheckConditionsOnSampleApp(sampleApp, args.ignoreyield)
        newstatus = ""
        details = ""
        if ready:
            if args.safe:
                logging.info("would launch: %s" % Repository.SampleAppSummary(sampleApp))
                logging.debug(AppServices.SimulateLaunch(sampleApp))
            else:
                # if we're ready, configure and launch
                logging.info("launching: %s" % Repository.SampleAppSummary(sampleApp))
                appSessionId = AppServices.ConfigureAndLaunchApp(sampleApp)
                logging.info("got app session id: %s" % appSessionId)
                Repository.SetNewSampleAppSessionId(sampleApp, appSessionId)
                newstatus = "submitted"
                details = "submission time: %s" % datetime.datetime.now()
        else:
            newstatus = "waiting"
            details = reason
            logging.debug("cannot launch: %s" % reason)
        if not args.safe:
            # this will only set the status if something has changed
            Repository.SetSampleAppStatus(sampleApp, newstatus, details)

    logging.debug("Finished launcher")
