"""
Create an app within the local configuration database so that these apps can be associated with samples.
"""
import json
import os
import sys

# Add relative path libraries
SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.append(os.path.abspath(os.path.sep.join([SCRIPT_DIR, "..", "lib"])))

APP_TYPES = ["SingleGenome", "TumourNormal", "FamilyTrio"]

from ConfigurationServices import ConfigurationServices
from DataAccessCreate import DataAccessCreate


class AppCreationException(Exception):
    pass


def get_args():
    """
    Setup, acquire and validate the command line arguments
    """
    import argparse

    parser = argparse.ArgumentParser(description='Create samples against a project')
    parser.add_argument('-n', '--name', type=str, dest="name", required=True, help='name of app')
    parser.add_argument('-p', '--properties', type=str, dest="properties", required=True,
                        help='json file containing properties for app')
    parser.add_argument('-e', '--defaults', type=str, dest="defaults", default="{}",
                        help='json file containing any default values for app properties')
    parser.add_argument('-u', '--inputdetails', type=str, dest="inputdetails", default="{}",
                        help='json file containing details for the input variables')
    parser.add_argument('-r', '--thresholds', type=str, dest="thresholds", required=True,
                        help='file containing json description of QC thresholds')
    parser.add_argument('-m', '--metricsfile', type=str, dest="metricsfile", required=True,
                        help='extension of file to use for QC checking')
    parser.add_argument('-y', '--type', type=str, dest="type", required=True,
                        help="app type (one of: %s)" % str(APP_TYPES))
    parser.add_argument('-d', '--deliverable', type=str, dest="deliverable", required=True,
                        help="comma separated list of file extensions to download as the deliverable")
    parser.add_argument('-b', '--basespaceid', type=str, required=True, dest="basespaceid",
                        help='ID of project in BaseSpace')
    parser.add_argument('-s', '--resultname', type=str, dest="resultname", default="",
                        help='Specify the app result name to be used for QC and deliverable download')

    args = parser.parse_args()
    assert args.type in APP_TYPES, "app type must be one of: %s" % APP_TYPES

    return args


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
            raise AppCreationException("improperly specified threshold: %s" % tname)


def validate_json_file(json_file):
    if not os.path.exists(json_file):
        raise AppCreationException("missing file: %s" % json_file)
    with open(json_file) as fh:
        json_text = fh.read()
        try:
            json.loads(json_text)
        except ValueError as e:
            raise AppCreationException("failed to parse json %s (%s)" % (json_file, str(e)))
    return json_text

if __name__ == "__main__":
    args = get_args()

    configuration_services = ConfigurationServices()
    db_config = configuration_services.get_config("DBFile")
    data_access_create = DataAccessCreate(db_config, configuration_services)

    properties = validate_json_file(args.properties)
    defaults = validate_json_file(args.defaults)
    thresholdtext = validate_json_file(args.thresholds)
    input_details = validate_json_file(args.inputdetails)
    validate_thresholds(thresholdtext)

    data_access_create.add_app(
        app_name=args.name,
        app_type=args.type,
        app_properties=properties,
        app_defaults=defaults,
        input_details=input_details,
        app_result_name=args.resultname,
        metrics_file=args.metricsfile,
        qc_thresholds=thresholdtext,
        deliverable_list=args.deliverable,
        basespace_id=args.basespaceid
    )
