"""
Simulate an app launch - pull out and fill in the template for an app. The generated json can be used to launch an app using, for example, curl or Postman.
"""

import os
import sys

# Add relative path libraries
SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.append(os.path.abspath(os.path.sep.join([SCRIPT_DIR, "..", "lib"])))

import Repository
import AppServices
import SampleServices

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='list sample apps with constraints and optionally update their status')
    parser.add_argument('-i', '--id', type=str, dest="id", required=True, help='attempt to launch just a specific SampleApp id')

    args = parser.parse_args()

    sampleApp = Repository.GetSampleAppByID(args.id)


    print AppServices.SimulateLaunch(sampleApp)
