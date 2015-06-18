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
sys.path.append(os.path.abspath(os.path.sep.join([SCRIPT_DIR, "..", "..", "basespace-python-sdk", "src"])))

from BaseSpacePy.api.BaseSpaceAPI import BaseSpaceAPI

from ConfigurationServices import ConfigurationServices
from DataAccessRead import DataAccessRead
from SampleServices import SampleServices
from AppServices import AppServices


def get_args():
    import argparse

    parser = argparse.ArgumentParser(description='Launch sample apps when ready')
    parser.add_argument('-i', '--id', type=str, dest="id", help='attempt to launch just a specific SampleApp id')
    parser.add_argument('-s', '--safe', dest="safe", default=False, action="store_true",
                        help='safe mode - say what you would do without doing it')
    parser.add_argument('-Y', '--ignoreyield', dest="ignoreyield", default=False, action="store_true",
                        help="ignore any missing yield")
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
        logfile = configuration_services.get_config("LAUNCHER_LOG_FILE")
        if not os.access(os.path.dirname(logfile), os.W_OK):
            print "log directory: %s does not exist or is not writeable" % (logfile)
            sys.exit(1)
        logging.basicConfig(filename=logfile, level=args.loglevel,
                            format=configuration_services.get_config("LogFormat"))

    #logging.basicConfig(level=args.loglevel)
    pl = logging.getLogger("peewee")
    pl.setLevel(logging.INFO)

    logging.debug("Starting launcher")

    if args.id:
        proto_apps = [data_access_read.get_proto_app_by_id(args.id)]
    else:
        # get all the SampleApps with the waiting status
        constraints = {"status": "waiting"}
        logging.debug("Finding samples")
        proto_apps = data_access_read.get_proto_apps_by_constraints(constraints)
        logging.debug("working on %d ProtoApps" % len(proto_apps))

    for proto_app in proto_apps:
        logging.debug("working on ProtoApp: %s" % proto_app)
        # unpack the SampleApp a little
        app_name = proto_app.app.name
        # check whether the SampleApp is ready to launch, including getting a reason if it isn't ready
        readiness_result = app_services.check_conditions_on_proto_app(proto_app, args.ignoreyield)
        newstatus = ""
        details = ""
        if readiness_result:
            if args.safe:
                logging.info("would launch: %s" % proto_app)
                logging.debug(app_services.simulate_launch(proto_app))
            else:
                # if we're ready, configure and launch
                logging.info("launching: %s" % proto_app)
                appSessionId = app_services.configure_and_launch_app(proto_app)
                logging.info("got app session id: %s" % appSessionId)
                proto_app.set_appsession_id(appSessionId)
                newstatus = "submitted"
                details = "submission time: %s" % datetime.datetime.now()
        else:
            newstatus = "waiting"
            details = readiness_result.details
            logging.debug("cannot launch: %s" % details)
        if not args.safe:
            # this will only set the status if something has changed
            proto_app.set_status(newstatus, details)

    logging.debug("Finished launcher")
