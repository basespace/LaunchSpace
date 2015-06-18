"""
Creates entries in the AppSupplies table

There are actually three ways an app can provide its output to a downstream app:

1. The whole app output. This will be true if an app only produces one app result and the downstream app consumes that
2. One appresult from within the app output. In this case, it should be possible to label this output
without providing the type or pathglob parameters
3. A file from within one appresult of an app's output. In this case, you need to specify type and pathglob
but appresultname is only needed if the app produces more than one appresult

In cases 2 and 3 it is always necessary to provide a label to the output we're going to chain against (the outputname)
this is then referred to in the workflow creation script with proto_app.output["--outputnamehere--"]

FIXME: specifying type 2 is currently unsupported!
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
    parser.add_argument('-t', '--type', type=str, dest="type", required=True, help='type of app output')
    parser.add_argument('-r', '--appresultname', type=str, dest="appresultname", default="",
                        help='for apps that produce more than one appresult, where can this output be found')
    parser.add_argument('-p', '--pathglob', type=str, dest="pathglob", required=True,
                        help='glob-path to output file within app result')

    args = parser.parse_args()

    configuration_services = ConfigurationServices()
    db_config = configuration_services.get_config("DBFile")
    data_access_read = DataAccessRead(db_config, configuration_services)
    data_access_create = DataAccessCreate(db_config, configuration_services)

    app = data_access_read.get_one_app_by_substring(args.appname)

    if app.get_output_by_name(args.outputname):
        data_access_create.update_app_output_description(app, args.outputname, args.type, args.appresultname, args.pathglob)
    else:
        data_access_create.add_app_output_description(app, args.outputname, args.type, args.appresultname, args.pathglob)
