"""
Accession samples into the local configuration database, either individually or by reading from a file
This also associates an app with the sample
"""

import os
import sys

# Add relative path libraries
SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.append(os.path.abspath(os.path.sep.join([SCRIPT_DIR, "..", "lib"])))

import Repository

class SampleCreationException(Exception):
    pass

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Create samples against a project')
    parser.add_argument('-p', '--project', type=str, required=True, dest="project", help='name of project')
    parser.add_argument('-f', '--file', type=str, dest="file", help="tsv file containing many samples")
    parser.add_argument('-l', '--lims', type=str, dest="lims", help="tsv containing a LIMS manifest generated by Clarity LIMS")
    # these manual adding options are not currently well supported, so best to just not expose them.
    parser.add_argument('-a', '--app', type=str, dest="app", help='name of app to attach to sample')
    parser.add_argument('-n', '--name', type=str, dest="name", help='name of sample')
    parser.add_argument('-r', '--related', type=str, dest="related", help="related sample name")
    parser.add_argument('-e', '--relationship', type=str, dest="relationship", help='name of relationship')

    args = parser.parse_args()

    if args.file and args.lims:
        print "don't specify both a file and a LIMS file!"
        sys.exit(1)

    projectName = args.project
    if args.file:
        samples, relationships = Repository.ConfigureSamplesFromFile(projectName, args.file)
    elif args.lims:
        samples, relationships = Repository.ConfigureSamplesFromLIMSFile(projectName, args.lims)
    elif args.name:
        if not args.app:
            print "if you specify a sample, you need to specify an app"
            sys.exit(1)
        # build up the variables we need to create the sample(s) and any specified relationships
        sampleName = args.name
        appName = args.app
        samples = set()
        relationships = set()
        sample = Repository.AddSample(sampleName, projectName)
        # keep track of the samples we build so we can report on them
        if sample:
            samples.add(sample)
        Repository.AddSampleApp(sampleName, appName)
        if args.related or args.relationship:
            if not args.related or not args.relationship:
                print "to add a relationship need both related sample and relationship name"
                sys.exit(1)
            # if a relationship has been specified, create the target sample...
            toSample = args.related
            relationshipName = args.relationship
            sample = Repository.AddSample(
                sampleName=toSample,
                projectName=projectName
            )
            if sample:
                samples.add(sample)
            # ... and then create the relationship
            relationship = Repository.AddSampleRelationship(
                fromSample=sampleName,
                toSample=toSample,
                relationshipName=relationshipName
            )
            relationships.add(relationship)

    else:
        print "need to specify a tsv file or a LIMS-generated tsv file"
        sys.exit(1)

    print "Created %i samples with %i relationships" % (len(samples), len(relationships))

    #Repository.DBApi.DBOrm.database.close()
    # assert(args.name), "must specify a file or a sample name!"
    # Repository.AddSample(
    #     sampleName=args.name, 
    #     projectName=args.project,
    #     relatedSample=args.related)
    # if args.app:
    #     Repository.AddSampleApp(args.name, args.app)

