"""
for an app that is already defined, add a QC and delivery step
"""
import json
import os
import sys

# Add relative path libraries
SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.append(os.path.abspath(os.path.sep.join([SCRIPT_DIR, "..", "lib"])))

from ConfigurationServices import ConfigurationServices
from DataAccessRead import DataAccessRead
from DataAccessCreate import DataAccessCreate
from LaunchSpaceUtil import validate_json_file


class QCAndDeliveryException(Exception):
    pass


def validate_thresholds(threshold_text):
    """
    unpack thresholds from a json string and raises an exception if they are not in the expected format

    @param json_text: (str) thresholds encoded as json

    @raises AppServicesException: if the json string is not in the expected format.
    """
    required_fields = {"operator", "threshold"}
    thresholds_ = json.loads(threshold_text)
    for tname in thresholds_:
        tdetails = thresholds_[tname]
        if set(tdetails.keys()) != required_fields:
            raise QCAndDeliveryException("improperly specified threshold: %s" % tname)


def get_args():
    """
    Setup, acquire and validate the command line arguments
    """
    import argparse

    parser = argparse.ArgumentParser(description='Create samples against a project')
    parser.add_argument('-n', '--appname', type=str, dest="appname", required=True, help='local name of app')
    parser.add_argument('-r', '--thresholds', type=str, dest="thresholds", required=True,
                        help='file containing json description of QC thresholds')
    parser.add_argument('-m', '--metricsfile', type=str, dest="metricsfile", required=True,
                        help='extension of file to use for QC checking')
    parser.add_argument('-d', '--deliverable', type=str, dest="deliverable", required=True,
                        help="comma separated list of file extensions to download as the deliverable")
    parser.add_argument('-s', '--resultname', type=str, dest="resultname", default="",
                        help='Specify the app result name to be used for QC and deliverable download')
    args = parser.parse_args()

    return args

if __name__ == "__main__":
    args = get_args()

    configuration_services = ConfigurationServices()
    db_config = configuration_services.get_config("DBFile")
    data_access_create = DataAccessCreate(db_config, configuration_services)
    data_access_read = DataAccessRead(db_config, configuration_services)

    thresholdtext = validate_json_file(args.thresholds)
    validate_thresholds(thresholdtext)

    apps = [data_access_read.get_app_by_name(args.appname)]
    assert (len(apps) == 1), "app name not specific enough: %s matches %s" % (
        args.appname, ",".join([app.name for app in apps]))

    app = apps[0]

    data_access_create.add_app_qc_and_delivery(
        app=app,
        app_result_name=args.resultname,
        metrics_file=args.metricsfile,
        qc_thresholds=thresholdtext,
        deliverable_list=args.deliverable
    )
