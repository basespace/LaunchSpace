"""
List the samples that are accessioned into the local configuration database. 
"""

import os
import sys

# Add relative path libraries
SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.append(os.path.abspath(os.path.sep.join([SCRIPT_DIR, "..", "lib"])))

from ConfigurationServices import ConfigurationServices
from DataAccessRead import DataAccessRead
from DataAccessDelete import DataAccessDelete

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-n', '--name', type=str, dest="name", help='name of sample')

    parser.add_argument('-D', '--delete', dest="delete", action="store_true", default=False,
                        help='delete selected Samples')
    args = parser.parse_args()

    configuration_services = ConfigurationServices()
    db_config = configuration_services.get_config("DBFile")
    data_access_read = DataAccessRead(db_config, configuration_services)

    if args.name:
        samples = [data_access_read.get_sample_by_name(args.name)]
    else:
        samples = data_access_read.get_all_samples_with_relationships()

    if args.delete:
        data_access_delete = DataAccessDelete(db_config, configuration_services)
        print "Deleting %s" % "\n".join(samples)
        data_access_delete.delete_samples(samples)
    else:
        data_access_read.print_sample_summaries(samples)
