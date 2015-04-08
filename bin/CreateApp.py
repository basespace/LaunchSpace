"""
Create an app within the local configuration database so that these apps can be associated with samples.
"""
import os
import sys

# Add relative path libraries
SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.append(os.path.abspath(os.path.sep.join([SCRIPT_DIR, "..", "lib"])))

import Repository
# needed to validate the qc threshold json blob
import AppServices

APP_TYPES = [ "SingleGenome", "TumourNormal", "FamilyTrio" ]

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Create samples against a project')
    parser.add_argument('-n', '--name', type=str, dest="name", required=True, help='name of app')
    parser.add_argument('-t', '--template', type=str, dest="template", required=True, help='file containing json template for app')
    parser.add_argument('-r', '--thresholds', type=str, dest="thresholds", required=True, help='file containing json description of QC thresholds')
    parser.add_argument('-m', '--metricsfile', type=str, dest="metricsfile", required=True, help='extension of file to use for QC checking')
    parser.add_argument('-y', '--type', type=str, dest="type", required=True, help="app type (one of: %s)" % str(APP_TYPES))
    parser.add_argument('-d', '--deliverable', type=str, dest="deliverable", required=True, help="comma separated list of file extensions to download as the deliverable")
    parser.add_argument('-b', '--basespaceid', type=str, required=True, dest="basespaceid", help='ID of project in BaseSpace')
    parser.add_argument('-s', '--resultname', type=str, dest="resultname", default="", help='Specify the app result name to be used for QC and deliverable download')

    args = parser.parse_args()

    assert args.type in APP_TYPES, "app type must be one of: %s" % APP_TYPES

    template = open(args.template).read()
    try:
        thresholdtext = open(args.thresholds).read()
        try:
            AppServices.ValidateThresholdsJson(thresholdtext)
        except AppServices.AppServicesException as ae:
            print "invalid threshold json file: %s" % (ae)
    except:
        print "problem reading thresholds file"
        raise

    Repository.AddApp(
        appName=args.name, 
        appType=args.type, 
        appTemplate=template,
        appResultName=args.resultname,
        metricsFile=args.metricsfile,
        qcThresholds=thresholdtext,
        deliverableList=args.deliverable,
        basespaceId=args.basespaceid
    )
