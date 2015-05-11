import os
import sys
import logging
from collections import defaultdict

# Add relative path libraries
SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.append(os.path.abspath(os.path.sep.join([SCRIPT_DIR, "..", "lib"])))

import Repository
import AppServices
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
        logfile = ConfigurationServices.GetConfig("QCCHECKER_LOG_FILE")
        if not os.access(os.path.dirname(logfile), os.W_OK):
            print "log directory: %s does not exist or is not writeable" % (logfile)
            sys.exit(1)
        logging.basicConfig(filename=logfile, level=args.loglevel, format=ConfigurationServices.GetConfig("LogFormat"))
    #logging.basicConfig(level=args.loglevel)

    pl = logging.getLogger("peewee")
    pl.setLevel(logging.INFO)

    logging.debug("Starting qc-checker")

    if args.id:
        sampleApps = [ Repository.GetSampleAppByID(args.id) ]
    else:
        # get all samples that are in the app-finished state
        constraints = { "status" : [ "app-finished" ] }
        sampleApps = Repository.GetSampleAppByConstraints(constraints)
        logging.info("Working on %i samples" % len(sampleApps))

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
        # apply automated QC to the SampleApp and record the failures
        failures = AppServices.apply_automated_qc_to_app_result(sampleApp)
        failuredetails = ";".join(failures)
        # use the failures to determine whether the SampleApp is qc-passed or not
        if failures:
            logging.debug("failed: %s" % failures)
            newstatus = "qc-failed"    
        else:
            newstatus = "qc-passed"
        if args.safe:
            logging.info("would update %s to: %s" % (Repository.SampleAppSummary(sampleApp), newstatus))
        else:
            transition = (Repository.SampleAppToStatus(sampleApp), newstatus)
            # failuredetails will be a blank string if there are no failures
            Repository.SetSampleAppStatus(sampleApp, newstatus, failuredetails)
            AppServices.set_qc_result_in_basespace(sampleApp, newstatus, failuredetails)
            transitions[transition].append(sampleAppId)

    # log how many of each transition we've made. If the number is low enough, report which apps have had each transition type
    for transition in sorted(transitions):
        if len(transitions[transition]) > 40:
            logging.info("%s : %i" % (transition, len(transitions[transition])))
        else:
            logging.info(
                "%s : %i (%s)" % (
                    transition, len(transitions[transition]), ", ".join([str(x) for x in transitions[transition]])))

    logging.debug("Finished qc-checker")

