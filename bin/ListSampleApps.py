"""
Tool to interrogate the local configuration database for apps associated with them.

This is the main tool to keep track of the status of analysis governed by LaunchSpace. 
As well as filtering the sampleapps that are displayed, this tool also allows the status for the selected sample/apps to be altered with the -S switch.
"""

import os
import sys

# Add relative path libraries
SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.append(os.path.abspath(os.path.sep.join([SCRIPT_DIR, "..", "lib"])))

import Repository
import ConfigurationServices

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='list sample apps with constraints and optionally update their status')
    # arguments that affect the query
    parser.add_argument('-i', '--id', type=str, dest="id", help='just lookup a specific SampleApp id')
    parser.add_argument('-n', '--name', type=str, dest="name", help='filter by name of app')
    parser.add_argument('-p', '--project', type=str, dest="project", help='filter by name of project')
    parser.add_argument('-s', '--sample', type=str, dest="sample", help='filter by name of sample')
    parser.add_argument('-u', '--status', type=str, dest="status", help='filter by SampleApp status')
    parser.add_argument('-x', '--exact', dest="exact", action="store_true", default=False, help='use exact matching of search terms')
    parser.add_argument('-y', '--type', type=str, dest="type", help='filter by app type')

    # arguments that affect the way the results are reported
    parser.add_argument('-e', '--showdetails', dest="showdetails", action="store_true", default=False, help='show status details, if any exist')

    # arguments that cause an update or deletion to the local database
    parser.add_argument('-D', '--delete', dest="delete", action="store_true", default=False, help='delete selected SampleApps')
    parser.add_argument('-S', '--newstatus', type=str, dest="newstatus", help='update the status of the selected SampleApps')

    args = parser.parse_args()

    # uncomment these lines to see the gory details of what Peewee is doing
    #import logging
    #logging.basicConfig(level=logging.DEBUG)

    constraints = {}
    if args.project:
        constraints["project"] = args.project
    if args.sample:
        constraints["sample"] = args.sample
    if args.status:
        constraints["status"] = [ args.status ]
    if args.type:
        constraints["type"] = args.type
    if args.id:
        constraints["id"] = args.id

    sampleApps = Repository.GetSampleAppByConstraints(constraints, args.exact)

    for sampleApp in sampleApps:
        if args.newstatus:
            try:
                Repository.SetSampleAppStatus(sampleApp, args.newstatus)
            except Repository.RepositoryException as e:
                print "failed to set status: %s" % str(e)
                sys.exit(1)
        elif args.delete:
            Repository.DeleteSampleApp(sampleApp)
            continue
        print Repository.SampleAppSummary(sampleApp, showDetails=args.showdetails)
