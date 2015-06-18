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
sys.path.append(os.path.abspath(os.path.sep.join([SCRIPT_DIR, "..", "..", "basespace-python-sdk", "src"])))

from BaseSpacePy.api.BaseSpaceAPI import BaseSpaceAPI

from ConfigurationServices import ConfigurationServices
from DataAccessRead import DataAccessRead
from SampleServices import SampleServices
from AppServices import AppServices


def get_args():
    import argparse

    parser = argparse.ArgumentParser(description='update status of sample/apps')
    parser.add_argument('-i', '--id', type=str, dest="id", help='update just a specific SampleApp id')
    parser.add_argument('-s', '--safe', dest="safe", default=False, action="store_true",
                        help='safe mode - say what you would do without doing it')
    parser.add_argument('-l', '--logtostdout', dest="logtostdout", default=False, action="store_true",
                        help="log to stdout instead of default log file")
    parser.add_argument("-L", "--loglevel", dest="loglevel", default="INFO",
                        help="loglevel, default INFO. Choose from WARNING, INFO, DEBUG")
    return parser.parse_args()


if __name__ == "__main__":
    args = get_args()

    baseSpaceAPI = BaseSpaceAPI()
    configuration_services = ConfigurationServices()
    db_config = configuration_services.get_config("DBFile")
    data_access_read = DataAccessRead(db_config, configuration_services)
    sample_services = SampleServices(baseSpaceAPI, configuration_services)
    app_services = AppServices(baseSpaceAPI, sample_services, configuration_services, data_access_read)

    if args.safe or args.logtostdout:
        logging.basicConfig(level=args.loglevel, format=configuration_services.get_config("LogFormat"))
    else:
        logfile = configuration_services.get_config("TRACKER_LOG_FILE")
        if not os.access(os.path.dirname(logfile), os.W_OK):
            print "log directory: %s does not exist or is not writeable" % logfile
            sys.exit(1)
        logging.basicConfig(filename=logfile, level=args.loglevel,
                            format=configuration_services.get_config("LogFormat"))
    #logging.basicConfig(level=args.loglevel)

    pl = logging.getLogger("peewee")
    pl.setLevel(logging.INFO)

    logging.debug("Starting tracker")

    if args.id:
        proto_apps = [data_access_read.get_proto_app_by_id(args.id)]
    else:
        # get all the SampleApps with statuses that the Tracker will be able to update
        # these represent "live" statuses on BaseSpace
        constraints = {"status": ["submitted", "pending", "running"]}
        proto_apps = data_access_read.get_proto_apps_by_constraints(constraints)
        logging.debug("Working on %i samples" % len(proto_apps))

    # there's quite a lot code shared here with QCChecker.py, to iterate over SampleApps and update them

    # record what transitions we make (state -> state for each SampleApp) so we can report at the end
    # all SampleApps will end up in either "qc-failed" or "qc-passed" states
    transitions = defaultdict(list)
    for proto_app in proto_apps:
        # unpack the SampleApp a little
        app_name = proto_app.app.name
        appsession_id = proto_app.basespaceid
        logging.debug("working on: %s" % proto_app)

        if not appsession_id:
            logging.warn("No BaseSpace Id for SampleApp: %s" % proto_app)
            continue
        # get the new status
        newstatus = app_services.get_app_status(appsession_id)
        if args.safe:
            logging.info("would update %s to: %s" % (proto_app, newstatus))
        else:
            # record the transition and update in the db
            transition = (proto_app.status, newstatus)
            proto_app.set_status(newstatus)
            transitions[transition].append(appsession_id)

    # log how many of each transition we've made.
    # If the number is low enough, report which apps have had each transition type
    for transition in sorted(transitions):
        if len(transitions[transition]) > 40:
            logging.info("%s : %i" % (transition, len(transitions[transition])))
        else:
            logging.info(
                "%s : %i (%s)" % (
                    transition, len(transitions[transition]), ", ".join([str(x) for x in transitions[transition]])))
    logging.debug("Finished tracker")

