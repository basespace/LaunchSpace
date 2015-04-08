"""
Services to access the configuration of domain entities like samples, projects and apps
this module has knowledge of how the entities are stored and their data structures
but abstracts away these mechanisms for any calling client

based on the Python module-as-singleton pattern proposed here:

http://stackoverflow.com/questions/31875/is-there-a-simple-elegant-way-to-define-singletons-in-python?lq=1
"""

import csv
import json
import os
import DBApi
import ConfigurationServices

class RepositoryException(Exception):
    pass

######
# Convert objects that have come from Repository calls into various related primitives
######

# SampleApp

# SampleApp direct members

def SampleAppToId(sampleApp):
    return sampleApp.id

def SampleAppToBaseSpaceId(sampleApp):
    return sampleApp.basespaceid

def SampleAppToStatus(sampleApp):
    return sampleApp.status

def SampleAppSummary(sampleApp, showDetails=False):
    if showDetails:
        if sampleApp.basespaceid:
            return "%s :: %s :: %s (%s) (basespaceid: %s) (%s) (%s)" % (sampleApp.app.name, sampleApp.sample.project.name, sampleApp.sample.name, sampleApp.id, sampleApp.basespaceid, sampleApp.status, sampleApp.statusdetails)
        else:
            return "%s :: %s :: %s (%s) (%s) (%s)" % (sampleApp.app.name, sampleApp.sample.project.name, sampleApp.sample.name, sampleApp.id, sampleApp.status, sampleApp.statusdetails)
    else:
        return "%s :: %s :: %s (%s) (%s)" % (sampleApp.app.name, sampleApp.sample.project.name, sampleApp.sample.name, sampleApp.id, sampleApp.status)

def SampleAppToStatusDetails(sampleApp):
    return sampleApp.statusdetails

# SampleApp members via at least one join, but not to the App table

def SampleAppToProjectId(sampleApp):
    return sampleApp.sample.project.basespaceid

def SampleAppToOutputDirectory(sampleApp):
    # maybe I shouldn't encode this here...
    outputpath = sampleApp.sample.project.outputpath
    appResultName = "%s.%s" % (sampleApp.sample.name, sampleApp.app.name)
    return os.path.join(outputpath, appResultName)

def SampleAppToSampleName(sampleApp):
    return sampleApp.sample.name

# SampleApp joined to App

def SampleAppToAppId(sampleApp):
    return sampleApp.app.basespaceid

def SampleAppToMetricsFile(sampleApp):
    return sampleApp.app.metricsfile

def SampleAppToAppResultName(sampleApp):
    return sampleApp.app.resultname

def SampleAppToDeliverableList(sampleApp):
    return sampleApp.app.deliverablelist.split(",")

def SampleAppToAppType(sampleApp):
    return sampleApp.app.type

def SampleAppToAppName(sampleApp):
    return sampleApp.app.name

def SampleAppToTemplate(sampleApp):
    return sampleApp.app.template

def SampleAppToQCThresholds(sampleApp):
    return json.loads(sampleApp.app.qcthresholds)

# Project

def ProjectToName(project):
    return project.name

def ProjectToOutputPath(project):
    return project.outputpath

def ProjectToBaseSpaceId(project):
    return project.basespaceid

def ProjectSummary(project):
    return "\t".join([ project.name, project.outputpath, project.basespaceid ])

# Sample

def SampleSummary(sample):
    summary = [ sample.name, sample.project.name ]
    try:
        sr = DBApi.GetSampleRelationship(sample)
        summary.extend([ sr.relationship, sr.tosample.name ])
    except DBApi.DBMissingException:
        pass
    return "\t".join(summary)

def SampleToSampleName(sample):
    return sample.name

# App

def AppToName(app):
    return app.name

def AppToType(app):
    return app.type

def AppToTemplate(app):
    return app.template

def AppToQCThresholds(app):
    return app.qcthresholds

def AppToQCThresholdsSummary(app):
    summary = ""
    thresholds = json.loads(app.qcthresholds)
    for tname in thresholds:
        tdetails = thresholds[tname]
        summary += "%s\t%s\t%s\n" % (tname, tdetails["operator"], tdetails["threshold"])
    return summary

def AppToMetricsFile(app):
    return app.metricsfile

def AppToDeliverableList(app):
    return app.deliverablelist

def AppToAppResultName(app):
    return app.resultname

######
# create entities
######

# most of these are pretty thin wrappers around DBApi.py
# except those that involve file parsing to create database entries

def AddApp(appName, appType, appTemplate, appResultName, metricsFile, qcThresholds, deliverableList, basespaceId):
    DBApi.AddApp(
        appName=appName, 
        appType=appType, 
        appTemplate=appTemplate, 
        appResultName=appResultName,
        metricsFile=metricsFile,
        qcThresholds=qcThresholds,
        deliverableList=deliverableList,
        basespaceId=basespaceId
    )

def AddSample(sampleName, projectName):
    return DBApi.AddSample(
        sampleName=sampleName, 
        projectName=projectName
    )

def AddSampleRelationship(fromSample, toSample, relationshipName):
    return DBApi.AddSampleRelationship(
                fromSampleName=fromSample,
                toSampleName=toSample,
                relationship=relationshipName
    )

def AddProject(projectName, outputPath, basespaceId):
    DBApi.AddProject(projectName, outputPath, basespaceId)

def ConfigureSamplesFromFile(projectName, infile):
    try:
        DBApi.GetProjectByName(projectName)
    except DBApi.DBMissingException:
        raise
    samples = set()
    relationships = set()
    with DBApi.DBOrm.database.transaction():
        reader = csv.reader(open(infile), delimiter="\t")
        relationships_to_make = []
        for row in reader:
            if len(row) == 4:
                sampleName, appName, relatedSample, relationship = row
                # gather up this relationship - we'll deal with it at the end
                relationships_to_make.append((sampleName, relatedSample, relationship))
            elif len(row) == 2:
                sampleName, appName = row
            else:
                raise RepositoryException("wrong number of columns in sample input file: %s" % row)
            sample = DBApi.AddSample(
                sampleName=sampleName, 
                projectName=projectName
            )
            if sample:
                samples.add(sample)
            DBApi.AddSampleApp(sampleName, appName)
        for relationship in relationships_to_make:
            fromSample, toSample, relationshipName = relationship
            # the first in the relationship must already have an entry from the loop above
            # but the second may not
            sample = DBApi.AddSample(
                sampleName=toSample,
                projectName=projectName
            )
            if sample:
                samples.add(sample)
            relationship = DBApi.AddSampleRelationship(
                fromSampleName=fromSample,
                toSampleName=toSample,
                relationship=relationshipName
            )
            relationships.add(relationship)
    return samples, relationships

def ConfigureSamplesFromLIMSFile(projectName, limsFile):

    HEADER_TAG = "TABLE HEADER"
    DATA_TAG = "SAMPLE ENTRIES"
    # generator function to strip the extraneous stuff from around a LIMS manifest
    # this means we can pass it straight into csv.DictReader 
    def ReadFileStripHeader(infile):
        with open(infile) as fh:
            line = fh.next()
            while HEADER_TAG not in line:
                line = fh.next()
            # we're on the line marking the header. 
            # Yield the *next* line, which will be the header itself.
            yield fh.next()
            # skip the line marking the end of the header
            line = fh.next()
            assert HEADER_TAG in line, "header line missing from expected place!"
            # next line should mark the start of the data
            line = fh.next()
            assert DATA_TAG in line, "data line missing from expected place!"
            # get the first record
            line = fh.next()
            while DATA_TAG not in line:
                yield line
                line = fh.next()
            # when we get to here we've reached the end of the data records and we're done

    SAMPLE_HEADER = "Sample/Name"
    MATCHED_HEADER = "UDF/Match Sample IDs"
    IS_TUMOUR_HEADER = "UDF/Is Tumor Sample"
    ANALYSIS_HEADER = "UDF/Analysis"
    TN_RELATIONSHIP_NAME = ConfigurationServices.GetConfig("TN_RELATIONSHIP_NAME")
    dr = csv.DictReader(ReadFileStripHeader(limsFile), delimiter="\t")
    samples = set()
    relationships = set()
    with DBApi.DBOrm.database.transaction():
        for row in dr:
            sampleName = row[SAMPLE_HEADER]
            pairName = row[MATCHED_HEADER]
            isTumour = row[IS_TUMOUR_HEADER]
            appName = row[ANALYSIS_HEADER]
            sample1 = DBApi.AddSample(sampleName, projectName)
            sample2 = DBApi.AddSample(pairName, projectName)
            if sample1: 
                samples.add(sample1)
            if sample2:
                samples.add(sample2)
            # the tumour sample owns the analysis!
            if isTumour == "TRUE":
                DBApi.AddSampleApp(sampleName, appName)
                relationship = DBApi.AddSampleRelationship(
                    fromSampleName = sampleName,
                    toSampleName = pairName,
                    relationship = TN_RELATIONSHIP_NAME
                )
                if relationship:
                    relationships.add(relationship)
    return samples, relationships

def AddSampleApp(sampleName, appName):
    if not DBApi.HasSample(sampleName):
        raise RepositoryException("cannot attach an app to a non-existent sample!")

    if not DBApi.HasApp(appName):
        raise RepositoryException("cannot attach non-existent app to sample!")

    DBApi.AddSampleApp(
        sampleName=sampleName, 
        appName=appName
    ) 

######
# read (query for) entities
######

def GetSampleAppByID(sampleAppId):
    return DBApi.GetSampleAppByID(sampleAppId)

def GetSampleAppByConstraints(constraints, exact=False):
    return DBApi.GetSampleAppByConstraints(constraints, exact)

def GetSampleAppMapping():
    return DBApi.GetSampleAppMapping()

def GetProjectByName(projectName):
    return DBApi.GetProjectByName(projectName)

def GetAllProjects():
    return DBApi.GetAllProjects()

def GetAllApps():
    return DBApi.GetAllApps()

def GetAppByName(appName):
    return DBApi.GetAppByName(appName)

def GetSampleByName(sampleName):
    return DBApi.GetSampleByName(sampleName)

def GetAllSamples():
    return DBApi.GetAllSamples()

def GetAllSamplesWithRelationships():
    return DBApi.GetAllSamplesWithRelationships()

def GetNormalForTumour(tumourSampleName):
    return DBApi.GetNormalForTumour(tumourSampleName)

######
# update values of entities
######

def SetNewSampleAppSessionId(sampleApp, appSessionId):
    sampleApp.basespaceid = appSessionId
    sampleApp.save()

def SetSampleAppStatus(sampleApp, newStatus, details=""):
    PERMITTED_STATUSES = ConfigurationServices.GetConfig("PERMITTED_STATUSES")
    if newStatus not in PERMITTED_STATUSES:
        raise RepositoryException("invalid status: %s" % newStatus)
    if sampleApp.status != newStatus or sampleApp.statusdetails != details:
        sampleApp.status = newStatus
        sampleApp.statusdetails = details
        sampleApp.save()

######
# delete entities
######

def DeleteSampleApp(sampleApp):
    sampleApp.delete_instance()

def DeleteSample(sample):
    sample.delete_instance()

def DeleteSamples(samples):
    with DBApi.DBOrm.database.transaction():
        for sample in samples:
            sample.delete_instance()