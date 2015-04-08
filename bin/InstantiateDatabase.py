"""
Instantiates the local configuration database. 

Should only need to be run once. Will give an error if one has already been instantiated.
"""

import os
import sys

# Add relative path libraries
SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.append(os.path.abspath(os.path.sep.join([SCRIPT_DIR, "..", "lib"])))

import DBOrm
import ConfigurationServices


if __name__ == "__main__":
	DBFile = ConfigurationServices.GetConfig("DBFile")
	if os.path.exists(DBFile):
		print "DBFile already exists: %s" % (DBFile)
		sys.exit(1)

	DBOrm.create_tables()