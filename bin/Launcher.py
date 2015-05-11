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
        sample_apps = [data_access_read.get_sample_app_by_id(args.id)]
    else:
        # get all the SampleApps with the waiting status
        constraints = {"status": "waiting"}
        logging.debug("Finding samples")
        sample_apps = data_access_read.get_sample_apps_by_constraints(constraints)
        logging.debug("working on %d samples" % len(sample_apps))

    for sample_app in sample_apps:
        # unpack the SampleApp a little
        sample_name = sample_app.sample.name
        app_name = sample_app.app.name
        # check whether the SampleApp is ready to launch, including getting a reason if it isn't ready
        ready, reason = AppServices.check_conditions_on_sample_app(sample_app, args.ignoreyield)
        newstatus = ""
        details = ""
        if ready:
            if args.safe:
                logging.info("would launch: %s" % sample_app)
                logging.debug(app_services.simulate_launch(sample_app))
            else:
                # if we're ready, configure and launch
                logging.info("launching: %s" % sample_app)
                appSessionId = app_services.configure_and_launch_app(sample_app)
                logging.info("got app session id: %s" % appSessionId)
                sample_app.set_appsession_id(appSessionId)
                newstatus = "submitted"
                details = "submission time: %s" % datetime.datetime.now()
        else:
            newstatus = "waiting"
            details = reason
            logging.debug("cannot launch: %s" % reason)
        if not args.safe:
            # this will only set the status if something has changed
            sample_app.set_status(newstatus, details)

    logging.debug("Finished launcher")
