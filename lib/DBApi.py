"""
Database routines (DAL)
"""

import DBOrm
from peewee import DoesNotExist, IntegrityError, JOIN_LEFT_OUTER
import ConfigurationServices
from memoize import memoized

DEFAULT_STATUS="waiting"
PERMITTED_STATUSES = ConfigurationServices.GetConfig("PERMITTED_STATUSES")

class DBException(Exception):
    pass

class DBExistsException(DBException):
    pass

class DBMissingException(DBException):
    pass

######
# Utility functions
######

def MakeLikeString(inStr):
    # these are *s and not %s because peewee uses GLOB instead of LIKE (for sqlite)
    # so you need a different wildcard operator
    return "*%s*" % inStr

def AugmentQuery(query, matchObject, queryTerm, exact):
    """
    augment a peewee query object with an additional condition

    http://peewee.readthedocs.org/en/latest/peewee/querying.html#selecting-multiple-records
    """
    if isinstance(queryTerm, list):
        query = query.where(matchObject << queryTerm)
    else:
        if exact:
            query = query.where(matchObject == queryTerm)
        else:
            query = query.where(matchObject % MakeLikeString(queryTerm))
    return query

######
# Create
######

def AddSample(sampleName, projectName):
    project = GetProjectByName(projectName)
    try:
        return DBOrm.Sample.create(
            name=sampleName,
            project=project
        )
    # this means that this sample already exists
    except IntegrityError:
        return None

def AddProject(projectName, outputPath, basespaceId):
    try:
        DBOrm.Project.create(
            name=projectName,
            outputpath=outputPath,
            basespaceid=basespaceId
        )
    except IntegrityError:
        raise DBExistsException("project already exists!")

def AddApp(appName, appType, appTemplate, appResultName, metricsFile, qcThresholds, deliverableList, basespaceId):
    try:
        DBOrm.App.create(
            name=appName,
            type=appType,
            template=appTemplate,
            resultname=appResultName,
            metricsfile=metricsFile,
            qcthresholds=qcThresholds,
            deliverablelist=deliverableList,
            basespaceid = basespaceId
        )
    except IntegrityError:
        raise DBExistsException("app already exists!")

def AddSampleApp(sampleName, appName, status=DEFAULT_STATUS):
    assert status in PERMITTED_STATUSES
    try:
        sample = GetSampleByName(sampleName)
        app = GetAppByName(appName)
        DBOrm.SampleApp.create(
            sample=sample,
            app=app,
            status=status,
        )
    except DBMissingException:
        raise 
    # this means that this sample/app pairing already exists
    except IntegrityError as e:
        pass


def AddSampleRelationship(fromSampleName, toSampleName, relationship):
    try:
        fromSample = GetSampleByName(fromSampleName)
        toSample = GetSampleByName(toSampleName)
        return DBOrm.SampleRelationship.create(
            fromsample=fromSample,
            tosample=toSample,
            relationship=relationship
        )
    except DBMissingException:
        raise
    # this means that this relationship already exists
    except IntegrityError:
        return None

######
# Read
######

# because these routines talk to peewee directly, they use the peewee "DoesNotExist" exception
# everybody else calls these and then uses the local DBMissingException

@memoized
def GetProjectByName(projectName):
    try:
        return DBOrm.Project.get(DBOrm.Project.name==projectName)
    except DoesNotExist:
        raise DBMissingException("missing project: %s" % projectName)

def GetAllProjects():
    return [project for project in DBOrm.Project.select()]

def GetAllSamples():
    return [sample for sample in DBOrm.Sample.select()]

def GetAllSamplesWithRelationships():
    # the big select and join here is important, because it means all the foreign key relationships are prepacked into the returned peewee objects
    # otherwise, peewee will make separate queries to resolve foreign key connections
    return [sample for sample in DBOrm.Sample.select(DBOrm.Sample, DBOrm.SampleRelationship).join(DBOrm.SampleRelationship, JOIN_LEFT_OUTER)]

def GetSampleByName(sampleName):
    try:
        return DBOrm.Sample.get(DBOrm.Sample.name==sampleName)
    except DoesNotExist:
        raise DBMissingException("missing sample: %s" % sampleName)

def HasSample(sampleName):
    try:
        GetSampleByName(sampleName)
        return True
    except DBMissingException:
        return False

def GetAllApps():
    return [app for app in DBOrm.App.select()]

def GetAppByName(appName):
    try:
        return DBOrm.App.get(DBOrm.App.name==appName)
    except DoesNotExist:
        raise DBMissingException("missing app: %s" % appName)

def HasApp(appName):
    try:
        GetAppByName(appName)
        return True
    except DBMissingException:
        return False

def GetSampleAppMapping():
    # we need to join here to convert IDs to names.
    # the peewee syntax is pretty gnarly. Hopefully it makes the query efficient
    # http://peewee.readthedocs.org/en/latest/peewee/querying.html#joining-on-multiple-tables
    query = DBOrm.SampleApp.select().join(DBOrm.Sample).switch(DBOrm.SampleApp).join(DBOrm.App)
    return [ (row.sample.name, row.app.name) for row in query ]

def GetSampleAppByID(sampleAppId):
    try:
        return DBOrm.SampleApp.get(DBOrm.SampleApp.id==sampleAppId)
    except DoesNotExist:
        raise DBMissingException("missing SampleApp: %s" % sampleAppId)

def GetSampleAppByConstraints(constraints, exact=False):
    # if we've selected by a particular ID, we don't need to check the other constraints
    if "id" in constraints:
        return [ GetSampleAppByID(constraints["id"]) ]
    # if we join here then it pulls down all the foreign key connections into the objects
    # this prevents excessive object dereference queries occuring in any downstream code
    # the peewee syntax is pretty gnarly. Hopefully it makes the query efficient
    # http://peewee.readthedocs.org/en/latest/peewee/querying.html#joining-on-multiple-tables
    query = (DBOrm.SampleApp.select(DBOrm.Sample, DBOrm.Project, DBOrm.SampleApp, DBOrm.App)
                    .join(DBOrm.Sample)
                    .join(DBOrm.Project)
                    .switch(DBOrm.SampleApp)
                    .join(DBOrm.App))
    # a fair amount of repetition here
    # but I decided I preferred this to a more convoluted generic mechanism
    if "project" in constraints:
        queryField = DBOrm.Project.name
        query = AugmentQuery(query, queryField, constraints["project"], exact)
    if "sample" in constraints:
        queryField = DBOrm.Sample.name
        query = AugmentQuery(query, queryField, constraints["sample"], exact)
    if "status" in constraints:
        queryField = DBOrm.SampleApp.status
        query = AugmentQuery(query, queryField, constraints["status"], exact)
    if "type" in constraints:
        queryField = DBOrm.App.type
        query = AugmentQuery(query, queryField, constraints["type"], exact)
    if "name" in constraints:
        queryField = DBOrm.App.name
        query = AugmentQuery(query, queryField, constraints["name"], exact)

    return [ x for x in query ]

def GetSampleRelationship(sample):
    try:
        sr = sample.samplerelationship
        # if there is not relationship, this will throw an exception
        fs = sr.fromsample
        return sr
    except:
        raise DBMissingException("sample relationship could not be found")

def GetNormalForTumour(sampleName):
    sample = GetSampleByName(sampleName)
    sr = DBOrm.SampleRelationship.get(
        DBOrm.SampleRelationship.fromsample==sample)
    assert sr.relationship == "TumourNormal"
    return sr.tosample


######
# Update
######

def SetSampleAppStatus(sampleApp, status):
    assert status in PERMITTED_STATUSES, "bad status: %s" % status
    sampleApp.status = status
    sampleApp.save()
