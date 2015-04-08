"""
List the samples that are accessioned into the local configuration database. 
"""

import os
import sys

# Add relative path libraries
SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.append(os.path.abspath(os.path.sep.join([SCRIPT_DIR, "..", "lib"])))

import Repository

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-n', '--name', type=str, dest="name", help='name of sample')

    parser.add_argument('-D', '--delete', dest="delete", action="store_true", default=False, help='delete selected Samples')
    args = parser.parse_args()

    if args.name:
        samples = [ Repository.GetSampleByName(args.name) ]
    else:
        samples = Repository.GetAllSamplesWithRelationships()

    if args.delete:
        print "Deleting %s" % "\n".join([ Repository.SampleToSampleName(sample) for sample in samples ])
        Repository.DeleteSamples(samples)
    else:
        with Repository.DBApi.DBOrm.database.transaction():
            for sample in samples:
                print "%s" % Repository.SampleSummary(sample)