"""
Services to access BaseSpace App information using BaseSpace v1pre3 API
"""

import os, sys
import jinja2
import json
import operator
import csv
import logging

# Add relative path libraries
SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, os.path.abspath(os.path.sep.join([SCRIPT_DIR, "..", "..", "basespace-python-sdk", "src"])))
sys.path.append(os.path.abspath(os.path.sep.join([SCRIPT_DIR, "..", "lib"])))

from BaseSpacePy.api.BaseSpaceAPI import BaseSpaceAPI
from BaseSpacePy.model.QueryParameters import QueryParameters
import ConfigurationServices
import SampleServices
import Repository

class AppServicesException(Exception):
    pass

baseSpaceAPI = BaseSpaceAPI()
noLimitQP = QueryParameters({ "Limit" : 1000 })

######
# routines to check conditions to determine whether SampleApps are ready for launch
######

def CheckConditionsOnSampleApp(sampleApp, ignoreYield=False):
    """
    Evaluate whether a SampleApp is ready to be launched
    this is dependent on the app type ("SingleGenome", or "TumourNormal")

    @param sampleApp: (DBOrm.SampleApp)
    @param ignoreYield: (bool)

    @return (bool): whether the conditions are met, (str): any details about why conditions are not met
    """
    # unpack some stuff from the objects
    projectId = Repository.SampleAppToProjectId(sampleApp)
    sampleName = Repository.SampleAppToSampleName(sampleApp)
    appType = Repository.SampleAppToAppType(sampleApp)
    if appType == "SingleGenome":
        # if this is just a build, just check this sample is ready to go
        return CheckConditionsOnSample(sampleName, projectId, ignoreYield)
    if appType == "TumourNormal":
        # if this is a subtraction, look up the two samples and check the readiness of both
        tumourName = sampleName
        tumourReady, tumourDetails = CheckConditionsOnSample(tumourName, projectId, ignoreYield)
        if not tumourReady:
            return False, "(Tumour: %s)" % tumourDetails
        normal = Repository.GetNormalForTumour(sampleName)
        normalName = Repository.SampleToSampleName(normal)
        normalReady, normalDetails = CheckConditionsOnSample(normalName, projectId, ignoreYield)
        if not normalReady:
            return False, "(Normal: %s)" % normalDetails
        return True, None
    # FIXME: if this is a trio, look for completed individual builds on all three samples 

def CheckConditionsOnSample(sampleName, projectId, ignoreYield):
    """
    check if the sample has any fastqs and if they are of enough yield

    @param sampleName: (str)
    @param projectId: (str)
    @param ignoreYield: (bool)

    @return (bool): whether the conditions are met, (str): any details about why conditions are not met
    """
    yieldThreshold = ConfigurationServices.GetConfig("MinimumYield")
    if not SampleServices.SampleHasData(sampleName, projectId):
        return False, "No data"
    sampleYield = SampleServices.GetSampleYield(sampleName, projectId)
    if sampleYield > yieldThreshold:
        return True, None
    else:
        if ignoreYield:
            return True, "Ignoring low yield!"
        return False, "Not enough yield (%d < %d)" % (sampleYield, yieldThreshold)

######
# routines to build app launch json from templates
######

def PopulateTemplate(template, template_vars):
    """
    Use jinja2 to fill in an app launch template

    @param template: (str) a json file, but with {{ variables }} to be filled in
    @param template_vars: (dict) a mapping from variable name to value

    @return (str): json appropriate for an app launch

    @raises AppServicesException if the template cannot be populated
    """
    template_vars["ApiVersion"] = ConfigurationServices.GetConfig("ApiVersion")
    t = jinja2.Template(template, undefined=jinja2.StrictUndefined)
    try:
        return t.render(template_vars)
    except jinja2.exceptions.UndefinedError as err:
        raise AppServicesException("missing variables for template: %s" % str(err))

def SetupTemplateVariables(sampleApp):
    """
    Pull out the appropriate variables from a SampleApp to fill in an app launch template

    @param sampleApp: (DBOrm.SampleApp)

    @return (dict): variables needed to populate an app launch template

    @raises AppServicesException if the app type is unknown
    """
    projectId = Repository.SampleAppToProjectId(sampleApp)
    sampleName = Repository.SampleAppToSampleName(sampleApp)
    # GetMostRecentSampleFromSampleName gets a BaseSpace Sample object and then we resolve the Id directly
    sampleId = SampleServices.GetMostRecentSampleFromSampleName(sampleName, projectId).Id
    appType = Repository.SampleAppToAppType(sampleApp)
    appName = Repository.SampleAppToAppName(sampleApp)
    templateVars = {}
    if appType == "SingleGenome":
        templateVars["SampleName"] = sampleName
        templateVars["SampleID"] = sampleId
        templateVars["ProjectID"] = projectId
        templateVars["AppName"] = appName
    elif appType == "TumourNormal":
        # the main sampleName is the tumour in this case
        tumourSampleName = sampleName
        tumourSampleId = sampleId
        normalSample = Repository.GetNormalForTumour(sampleName)
        normalSampleName = Repository.SampleToSampleName(normalSample)
        normalSampleId = SampleServices.GetMostRecentSampleFromSampleName(normalSampleName, projectId).Id
        templateVars["TumourSampleName"] = tumourSampleName
        templateVars["TumourSampleID"] = tumourSampleId
        templateVars["NormalSampleName"] = normalSampleName
        templateVars["NormalSampleID"] = normalSampleId
        templateVars["ProjectID"] = projectId
        templateVars["AppName"] = appName
    else:
        raise AppServicesException("Unsupported app type: %s" % appType)
    return templateVars

def SampleAppToPopulatedTemplate(sampleApp):
    """
    Derive a populated app launch json from a template for a given SampleApp

    @param sampleApp: (DBOrm.SampleApp)

    @return (str): json appropriate for an app launch
    """
    template = Repository.SampleAppToTemplate(sampleApp)
    launchVars = SetupTemplateVariables(sampleApp)
    return PopulateTemplate(template, launchVars)

######
# app launch and tracking
######

def LaunchApp(appId, configJson):
    """
    Call BaseSpace to launch an app

    @param appId: (str) The BaseSpace ID of the app
    @param configJson: (str) A string encoded json object with the app launch details

    @return (str): the app session ID of the launched app

    @raises AppServicesException if the launch fails
    """
    try:
        # this should return the session ID
        return baseSpaceAPI.launchApp(appId, configJson).Id
    except Exception as e:
        raise AppServicesException("App launch failed: %s" % str(e))

def ConfigureAndLaunchApp(sampleApp):
    """
    configure and launch the app for a particular SampleApp

    @param sampleApp: (DBOrm.SampleApp)

    @return (str): the app session ID of the launched app
    """
    populatedTemplate = SampleAppToPopulatedTemplate(sampleApp)
    # loading and dumping the json removes one level of quoting,
    # which seems to break the API call if present.
    populatedTemplate = json.dumps(json.loads(populatedTemplate))
    appId = Repository.SampleAppToAppId(sampleApp)
    return LaunchApp(appId, populatedTemplate)

def SimulateLaunch(sampleApp):
    """
    return the app launch template that would be used for a particular SampleApp

    @param sampleApp: (DBOrm.SampleApp)

    @return (str): the app launch json
    """
    return SampleAppToPopulatedTemplate(sampleApp)

def GetAppStatus(appSessionId):
    """
    Call BaseSpace to find the status of an app session

    @param appSessionId: (str)

    @return (str): the app status. One of config.PERMITTED_STATUSES

    @raises AppServicesException if the app status from BaseSpace is not recognised
    """
    bsStatus = baseSpaceAPI.getAppSession(appSessionId).Status
    mapping = ConfigurationServices.GetConfig("STATUS_MAPPING")
    try:
        status = mapping[bsStatus]
    except KeyError:
        raise AppServicesException("Unknown app session status: %s" % bsStatus)
    return status


######
# Automated QC 
######

def ValidateThresholdsJson(jsonText):
    """
    unpack thresholds from a json string and raises an exception if they are not in the expected format

    @param jsonText: (str) thresholds encoded as json

    @raises AppServicesException: if the json string is not in the expected format.
    """
    REQUIRED_FIELDS = set([ "operator", "threshold" ])
    thresholds = json.loads(jsonText)
    for tname in thresholds:
        tdetails = thresholds[tname]
        if set(tdetails.keys()) != REQUIRED_FIELDS:
            raise AppServicesException("improperly specified threshold: %s" % tname)

def _CompareQCResultToThresholds(qcResults, thresholds):
    """
    compare the qcresults from a finished app to some appropriate thresholds
    each threshold entry has a value and an operator. If $(<metric> <operator> <value>) (eg. insert_size ge 300)
    the metric passes qc otherwise it fails

    @param qcResults: (dict) metric_name->value mapping for an app result
    @param thresholds: (dict) metric_name->metric_details. 

    @return (list of str): descriptions of the failing metrics

    @raises AppServicesException: if a required metric is missing
    """
    failures = []
    for metricName in thresholds:
        if metricName not in qcResults:
            raise AppServicesException("Metric missing from qc results: %s %s" % (metricName, qcResults))
        thresholdDetails = thresholds[metricName]
        metricOperator = thresholdDetails["operator"]
        metricValue = thresholdDetails["threshold"]
        operatorFunction = getattr(operator, metricOperator)
        # try/except partly to catch problems with the metrics - eg. NA values
        try:
            if not operatorFunction(qcResults[metricName], metricValue):
                failureMessage = "%s (%s %s %s)" % (metricName, qcResults[metricName], operatorFunction.__name__, metricValue)
                failures.append(failureMessage)
        except Exception as e:
            failureMessage = "%s (%s %s %s) (%s)" % (metricName, qcResults[metricName], operatorFunction.__name__, metricValue, str(e))
            failures.append(failureMessage)
    return failures

def _ReadQCResult(qcFile):
    """
    business logic for reading metrics from a file and packing them into a dictionary
    this business logic is currently selected by file extension and has been tested against the Isaac V2 app
    and the tumour/normal app. This method may need to be extended to support QC for other app types.

    @param qcFile: (filepath)

    @return (dict): metric->value

    @raises AppServicesException: if the metrics file is of unknown type (extension)
    """
    qcValues = {}
    # really crude handling of different file types. Refactor later if desired.
    if qcFile.endswith(".csv"):
        # assumes each row is a key/value pair
        reader = csv.reader(open(qcFile))
        for row in reader:
            if len(row) != 2:
                continue
            try:
                qcValues[row[0].strip(":")] = float(row[1].strip("%"))
            except ValueError:
                continue
    # this assumes a specific format of json based on the tumour/normal output
    # the tumour/normal output has several top-level entries, each of which is a table.
    # this code "flattens" these tables into namespaced elements like 
    # VariantStatsTable.Insertions.dbSNP
    elif qcFile.endswith(".json"):
        metrics = json.load(open(qcFile))
        for metricType in metrics:
            metricDetails = metrics[metricType]
            if "header" in metricDetails:
                headers = metricDetails["header"]
            elif "tableColumns" in metricDetails:
                headers = metricDetails["tableColumns"]
            else:
                continue
            assert "rows" in metricDetails, "expected to find rows in metrics details"
            rows = metricDetails["rows"]
            for row in rows:
                for colIndex in range(1, len(headers)):
                    columnName = headers[colIndex]
                    rowName = row[0]
                    rowColValue = row[colIndex]
                    flatName = "%s.%s.%s" % (metricType, columnName, rowName)
                    qcValues[flatName] = rowColValue
    else:
        raise AppServicesException("unknown extension on QC file: %s" % qcFile)
    return qcValues


def ApplyAutomatedQCToAppResult(sampleApp):
    """
    Assesses the QC status of an app result from a SampleApp

    @param sampleApp: (DBOrm.SampleApp)

    @return (list of str): descriptions of the failing metrics

    @raises AppServicesException: if the app results do not look as expected
    """
    thresholds = Repository.SampleAppToQCThresholds(sampleApp)
    metricsFile = Repository.SampleAppToMetricsFile(sampleApp)
    outputDir = Repository.SampleAppToOutputDirectory(sampleApp)
    basespaceId = Repository.SampleAppToBaseSpaceId(sampleApp)
    appResultName = Repository.SampleAppToAppResultName(sampleApp)
    qcDirName = ConfigurationServices.GetConfig("SAMPLE_LOG_DIR_NAME")
    qcPath = os.path.join(outputDir, qcDirName)
    # make directory to write qc file into
    if not os.path.exists(qcPath):
        os.makedirs(qcPath)
    logging.debug("retrieving basespace files with extension %s from appsession Id %s" % (metricsFile, basespaceId))
    qcFiles = baseSpaceAPI.downloadAppResultFilesByExtension(basespaceId, metricsFile, qcPath, appResultName, noLimitQP)
    if len(qcFiles) != 1:
        raise AppServicesException("did not get exactly one metrics file for QC!")
    qcFile = qcFiles[0]
    qcFilePath = os.path.join(qcPath, os.path.basename(qcFile.Path))
    logging.debug("got file: %s" % qcFilePath)
    qcResults = _ReadQCResult(qcFilePath)
    failures = _CompareQCResultToThresholds(qcResults, thresholds)
    return failures

def SetQCResultInBaseSpace(sampleApp, qcResult, details=""):
    """
    uses BaseSpace properties to store the qc result within BaseSpace itself

    https://developer.basespace.illumina.com/docs/content/documentation/rest-api/api-reference#Properties

    @param sampleApp: (DBOrm.SampleApp)
    @param qcResult: (bool)
    @param details: (str) why the qc failed

    @raises AppServicesException: if the BaseSpace call fails for any reason
    """
    basespaceId = Repository.SampleAppToBaseSpaceId(sampleApp)
    namespace = ConfigurationServices.GetConfig("QC_NAMESPACE")
    try:
        qcPayload = { "QCResult" : str(qcResult) }
        if details:
            qcPayload["QCDetails"] = str(details)
        pr = baseSpaceAPI.setResourceProperties("appsessions", basespaceId, qcPayload, namespace)
    except Exception as e:
        raise AppServicesException("failed to set QC properties for appsession: %s (%s)" % (basespaceId, str(e)))

######
# Download
######

def DownloadDeliverable(sampleApp):
    """
    download the configured deliverable file extensions for a given SampleApp

    @param sampleApp: (DBOrm.SampleApp)

    @raises AppServicesException: if any parts of the download fail 
    """
    outputDir = Repository.SampleAppToOutputDirectory(sampleApp)
    deliverableList = Repository.SampleAppToDeliverableList(sampleApp)
    basespaceId = Repository.SampleAppToBaseSpaceId(sampleApp)
    appResultName = Repository.SampleAppToAppResultName(sampleApp)
    for deliverableExtension in deliverableList:
        logging.info("downloading extension: %s" % deliverableExtension)
        try:
            downloadFiles = baseSpaceAPI.downloadAppResultFilesByExtension(basespaceId, deliverableExtension, outputDir, appResultName, noLimitQP)
        except Exception as e:
            raise AppServicesException("failed to download file: %s (%s)" % (deliverableExtension, str(e)))

