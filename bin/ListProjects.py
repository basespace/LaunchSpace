"""
Display information about projects accessioned into the local configuration database
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
    parser.add_argument('-n', '--name', type=str, dest="name", help='name of project')

    args = parser.parse_args()

    if args.name:
        projects = [ Repository.GetProjectByName(args.name) ]
    else:
        projects = Repository.GetAllProjects()

    for project in projects:
        print "%s" % Repository.ProjectSummary(project)