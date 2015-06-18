"""
Simulate an app launch - pull out and fill in the template for an app. The generated json can be used to launch an app using, for example, curl or Postman.
"""

import os
import sys

# Add relative path libraries
SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.append(os.path.abspath(os.path.sep.join([SCRIPT_DIR, "..", "lib"])))
sys.path.append(os.path.abspath(os.path.sep.join([SCRIPT_DIR, "..", "..", "basespace-python-sdk", "src"])))

from BaseSpacePy.api.BaseSpaceAPI import BaseSpaceAPI

from ConfigurationServices import ConfigurationServices
from DataAccessRead import DataAccessRead
from SampleServices import SampleServices
from AppServices import AppServices


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='list sample apps with constraints and optionally update their status')
    parser.add_argument('-i', '--id', type=str, dest="id", required=True, help='attempt to launch just a specific SampleApp id')

    args = parser.parse_args()

    # this is a little ridiculous, because we need all these objects just to get at the simulate method :(
    baseSpaceAPI = BaseSpaceAPI()
    configuration_services = ConfigurationServices()
    db_config = configuration_services.get_config("DBFile")
    data_access_read = DataAccessRead(db_config, configuration_services)
    sample_services = SampleServices(baseSpaceAPI, configuration_services)
    app_services = AppServices(baseSpaceAPI, sample_services, configuration_services, data_access_read)

    sample_app = data_access_read.get_proto_app_by_id(args.id)

    print app_services.simulate_launch(sample_app)
