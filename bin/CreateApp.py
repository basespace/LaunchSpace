"""
Create an app within the local configuration database so that these apps can be associated with samples.
"""
import json
import os
import sys
import pprint

# Add relative path libraries
SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.append(os.path.abspath(os.path.sep.join([SCRIPT_DIR, "..", "lib"])))
sys.path.append(os.path.abspath(os.path.sep.join([SCRIPT_DIR, "..", "..", "basespace-python-sdk", "src"])))

from BaseSpacePy.api.BaseSpaceAPI import BaseSpaceAPI

from ConfigurationServices import ConfigurationServices
from DataAccessCreate import DataAccessCreate
from LaunchSpaceUtil import validate_json_file
from BaseMountInterface import BaseMountInterface
from AppSessionMetaData import AppSessionMetaDataRaw, AppSessionMetaDataSDK


class AppCreationException(Exception):
    pass


def get_args():
    """
    Setup, acquire and validate the command line arguments
    """
    import argparse

    parser = argparse.ArgumentParser(description='Create samples against a project')
    parser.add_argument('-n', '--name', type=str, dest="name", help='name of app')
    parser.add_argument('-p', '--properties', type=str, dest="properties",
                        help='json file containing properties for app')
    parser.add_argument('-e', '--defaults', type=str, dest="defaults", default="{}",
                        help='json file containing any default values for app properties')
    parser.add_argument('-u', '--inputdetails', type=str, dest="inputdetails", default="{}",
                        help='json file containing details for the input variables')
    parser.add_argument('-a', '--appsessionid', dest="appsessionid", help='app session ID to use to derive an app spec')
    parser.add_argument('-m', '--appsessionpath', dest='appsessionpath',
                        help='BaseMount path to an appsession to derive an app spec')
    parser.add_argument('-b', '--basespaceid', type=str, dest="basespaceid",
                        help='ID of app in BaseSpace')
    parser.add_argument('-v', '--verbose', dest="verbose", action="store_true", default=False, help='verbose mode')
    parser.add_argument('-j', '--jsonfile', dest="jsonfile", help='json file containing apps to import')

    args = parser.parse_args()

    return args


if __name__ == "__main__":
    args = get_args()

    configuration_services = ConfigurationServices()
    db_config = configuration_services.get_config("DBFile")
    data_access_create = DataAccessCreate(db_config, configuration_services)

    if args.jsonfile:
        with open(args.jsonfile) as fh:
            allapps = json.load(fh)
            data_access_create.add_apps_from_blob(allapps)
    elif args.properties:
        properties = validate_json_file(args.properties)
        defaults = validate_json_file(args.defaults)
        input_details = validate_json_file(args.inputdetails)
        data_access_create.add_app(
            app_name=args.name,
            app_properties=properties,
            app_defaults=defaults,
            input_details=input_details,
            basespace_id=args.basespaceid
        )
    else:
        if args.appsessionid:
            api = BaseSpaceAPI()
            appsession_metadata = AppSessionMetaDataSDK(api.getAppSessionById(args.appsessionid))
        # or from the metaBSFS cached metadata
        elif args.appsessionpath:
            mbi = BaseMountInterface(args.appsessionpath)
            appsession_metadata = AppSessionMetaDataRaw(mbi.get_meta_data())
        else:
            print "must specify either a file containing properties (-p), an appsession id (-a) or an appsessionpath in metaBSFS (-m)"
            sys.exit(1)
        properties, defaults = appsession_metadata.get_refined_appsession_properties()
        # and a name for the app
        app_name = appsession_metadata.get_app_name()
        app_id = appsession_metadata.get_app_id()
        # finally, turn the properties and defaults into strings
        pstr = json.dumps(properties)
        if args.verbose:
            pprint.pprint(properties)
            pprint.pprint(defaults)
        defaults_str = json.dumps(defaults)
        data_access_create.add_app(
            app_name=app_name,
            app_properties=pstr,
            app_defaults=defaults_str,
            input_details={},
            basespace_id=app_id
        )
