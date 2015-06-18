"""
Augment entries in the AppConsumes table

Where an app consumes a file, it's useful to specify what type of file it should be
this way, when chained workflows are being specified we can throw an error
if people try to wire the wrong file into the input of the consuming app
"""
import os
import sys

# Add relative path libraries
SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.append(os.path.abspath(os.path.sep.join([SCRIPT_DIR, "..", "lib"])))

from ConfigurationServices import ConfigurationServices
from DataAccessRead import DataAccessRead
from DataAccessCreate import DataAccessCreate

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-n', '--appname', type=str, dest="appname", required=True, help='local name of app')
    parser.add_argument('-p', '--parametername', type=str, dest="parametername", required=True,
                        help='name of app parameter')
    parser.add_argument('-d', '--description', type=str, dest="description", required=True,
                        help='describe the app input parameter (provide a type)')

    args = parser.parse_args()

    configuration_services = ConfigurationServices()
    db_config = configuration_services.get_config("DBFile")
    data_access_read = DataAccessRead(db_config, configuration_services)
    data_access_create = DataAccessCreate(db_config, configuration_services)

    app = data_access_read.get_one_app_by_substring(args.appname)

    data_access_create.update_app_input_description(app, args.parametername, args.description)
