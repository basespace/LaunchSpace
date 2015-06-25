"""
List information about the apps that are accessioned into the local configuration database
"""
import json
import os
import sys

# Add relative path libraries
SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.append(os.path.abspath(os.path.sep.join([SCRIPT_DIR, "..", "lib"])))

from ConfigurationServices import ConfigurationServices
from DataAccessRead import DataAccessRead

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-n', '--name', type=str, dest="name", help='local name of app')
    parser.add_argument('-j', '--json', dest="json", action="store_true", help='dump as json')
    parser.add_argument('-f', '--asfunctions', dest="asfunctions", action="store_true", help='output the specification the app provides to WorkflowBuilder objects')

    args = parser.parse_args()

    configuration_services = ConfigurationServices()
    db_config = configuration_services.get_config("DBFile")
    data_access_read = DataAccessRead(db_config, configuration_services)

    if args.name:
        apps = data_access_read.get_all_apps_by_substring(args.name)
    else:
        apps = data_access_read.get_all_apps()

    allapps = []
    for app in apps:
        if args.json:
            allapps.append(app.to_dict())
        elif args.asfunctions:
            print app.get_as_function_summary()
        else:
            print app
            print "==="

    if args.json:
        print json.dumps(allapps)
