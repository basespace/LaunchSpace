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
from DataAccessCreate import DataAccessCreate

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-n', '--appname', type=str, dest="appname", required=True, help='local name of app')
    parser.add_argument('-o', '--outputname', type=str, dest="outputname", required=True, help='name of app output')
    parser.add_argument('-r', '--appresultname', type=str, dest="appresultname", required=True,
                        help='for apps that produce more than one appresult, where can this output be found')
    parser.add_argument('-p', '--pathglob', type=str, dest="pathglob", required=True,
                        help='glob-path to output file within app result')

    args = parser.parse_args()

    configuration_services = ConfigurationServices()
    db_config = configuration_services.get_config("DBFile")
    data_access_read = DataAccessRead(db_config, configuration_services)
    data_access_create = DataAccessCreate(db_config, configuration_services)

    apps = [data_access_read.get_app_by_name(args.appname)]
    assert (len(apps) == 1), "app name not specific enough: %s matches %s" % (
        args.appname, ",".join([app.name for app in apps]))

    app = apps[0]

    data_access_create.add_app_output_description(app, args.outputname, args.appresultname, args.pathglob)
