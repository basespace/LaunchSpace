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
    parser.add_argument('-i', '--id', type=str, dest="id", help='just lookup a specific ProtoApp id')

    args = parser.parse_args()

    configuration_services = ConfigurationServices()
    db_config = configuration_services.get_config("DBFile")
    data_access_read = DataAccessRead(db_config, configuration_services)

    constraints = {}
    if args.id:
        constraints["id"] = args.id

    proto_apps = data_access_read.get_proto_apps_by_constraints(constraints)

    for proto_app in proto_apps:
        dependencies = proto_app.get_dependencies()
        for dependency in dependencies:
            print dependency