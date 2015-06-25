"""
Instantiates the local configuration database. 

Should only need to be run once. Will give an error if one has already been instantiated.
"""

import os
import sys

# Add relative path libraries
SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.append(os.path.abspath(os.path.sep.join([SCRIPT_DIR, "..", "lib"])))

from ConfigurationServices import ConfigurationServices
from DataAccessLayer import DataAccessLayer
from DataAccessCreate import DataAccessCreate

if __name__ == "__main__":
    configuration_services = ConfigurationServices()
    db_file = configuration_services.get_config("DBFile")
    if os.path.exists(db_file):
        print "DBFile already exists: %s" % db_file
        sys.exit(1)
    data_access = DataAccessLayer(db_file, configuration_services)
    data_access_create = DataAccessCreate(db_file, configuration_services)

    data_access.create_tables()

    appblob = os.path.join(SCRIPT_DIR, "../data/appblob.json")
    if os.path.exists(appblob):
    	print "initialising apps from file: %s" % appblob
    	data_access_create.add_apps_from_blob()