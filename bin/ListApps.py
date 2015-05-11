"""
List information about the apps that are accessioned into the local configuration database
"""
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

    args = parser.parse_args()

    configuration_services = ConfigurationServices()
    db_config = configuration_services.get_config("DBFile")
    data_access_read = DataAccessRead(db_config, configuration_services)

    if args.name:
        apps = [data_access_read.get_app_by_name(args.name)]
    else:
        apps = data_access_read.get_all_apps()

    for app in apps:
        print app
        print "==="