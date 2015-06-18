"""
Services to access BaseSpace App information using BaseSpace v1pre3 API
"""

import os, sys
import jinja2
import json
import operator
import csv
import logging
import fnmatch

# Add relative path libraries
SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, os.path.abspath(os.path.sep.join([SCRIPT_DIR, "..", "..", "basespace-python-sdk", "src"])))
sys.path.append(os.path.abspath(os.path.sep.join([SCRIPT_DIR, "..", "lib"])))

from BaseSpacePy.api.BaseSpaceAPI import BaseSpaceAPI
from BaseSpacePy.model.QueryParameters import QueryParameters
from LaunchSpecification import LaunchSpecification
from DependencyChecking import app_result_ready_checker_factory, merge_dependency_readiness_results


class AppServicesException(Exception):
    pass


class AppServices(object):
    NO_LIMIT_QP = QueryParameters({"Limit": 1000})

    def __init__(self, basespace_api, sample_services, configuration_services, data_access_read):
        self._basespace_api = basespace_api
        # FIXME: only one method needs this (check_conditions_on_proto_app) so make it a parameter of that method
        self._sample_services = sample_services
        self._configuration_services = configuration_services
        self._data_access_read = data_access_read

    ######
    # routines to check conditions to determine whether ProtoApps are ready for launch
    ######

    def check_conditions_on_proto_app(self, proto_app, ignore_yield=False):
        """
        Evaluate whether a SampleApp is ready to be launched
        this is dependent on the app type ("SingleGenome", or "TumourNormal")

        @param proto_app: (DataAccessLayer.DataAccessLayer.ProtoApp)
        @param ignore_yield: (bool)

        @return (bool): whether the conditions are met, (str): any details about why conditions are not met
        """
        dependencies = proto_app.get_dependencies()
        readiness_results = []
        for dependency in dependencies:
            if dependency.sampledependency:
                sample_name = dependency.sampledependency.name
                project_id = proto_app.project.basespaceid
                readiness_result = self._sample_services.check_conditions_on_sample(sample_name, project_id,
                                                                                    ignore_yield)
            elif dependency.protoappdependency:
                # FIXME: at this point, we should look to see if this is a file dependency
                # and if so, use this to do a better check on the upstream app - to check whether it made this file
                dependent_pa = dependency.protoappdependency
                app_result_ready_checker = app_result_ready_checker_factory(dependent_pa.app.name, self)
                readiness_result = app_result_ready_checker.is_app_ready(dependent_pa)
            else:
                raise AppServicesException("Improper protoapp dependency!")
            readiness_results.append(readiness_result)
        return merge_dependency_readiness_results(readiness_results)

    ######
    # app launch and tracking
    ######

    def get_app_launch_properties(self, proto_app):
        project_basespace_id = proto_app.project.basespaceid
        # this should probably be its own object type
        launch_properties = dict()
        dependencies = proto_app.get_dependencies()
        # there are three types of dependencies - samples, appresults and files
        for dependency in dependencies:
            # we need to look up the parameter name to see what type it is
            appconsumes = dependency.get_app_consumes()
            if appconsumes.islistproperty and dependency.parametername not in launch_properties:
                launch_properties[dependency.parametername] = []
            if appconsumes.type == "sample":
                sample = dependency.sampledependency
                if sample is None:
                    raise AppServicesException("Trying to resolve empty sample dependency")
                parameter_basespaceid = self._sample_services.get_basespace_sample_id(sample.name, project_basespace_id)
            elif appconsumes.type == "appresult" or appconsumes.type == "file":
                pa = dependency.protoappdependency
                if pa is None:
                    raise AppServicesException("Trying to resolve empty protoapp dependency")
                # this should not happen because condition checking should happen first
                if not pa.basespaceid:
                    raise AppServicesException("ProtoApp dependency has not run yet!")
                app_supplies_details = dependency.get_app_supplies()
                if appconsumes.type == "appresult":
                    if app_supplies_details:
                        parameter_basespaceid = self.app_session_id_to_app_result_id(pa.basespaceid,
                                                                                     app_supplies_details.resultname)
                    else:
                        parameter_basespaceid = self.app_session_id_to_app_result_id(pa.basespaceid)
                elif appconsumes.type == "file":
                    parameter_basespaceid = self.get_dependency_file_id(pa.basespaceid, app_supplies_details)
                else:
                    raise AppServicesException("Unknwon parameter type: %s" % appconsumes.type)
            if appconsumes.islistproperty:
                launch_properties[dependency.parametername].append(parameter_basespaceid)
            else:
                launch_properties[dependency.parametername] = parameter_basespaceid
        launch_properties["project-id"] = proto_app.project.basespaceid
        return launch_properties

    def make_proto_app_launch_payload(self, proto_app):
        launch_specification = LaunchSpecification(proto_app.app.get_properties_as_dict(),
                                                   proto_app.app.get_defaults_as_dict(),
                                                   self._configuration_services)
        launch_properties = self.get_app_launch_properties(proto_app)
        launch_name = proto_app.get_distinctive_name()
        launch_json = launch_specification.make_launch_json(launch_properties, launch_name)
        return launch_json

    def launch_app(self, app_id, config_json):
        """
        Call BaseSpace to launch an app

        @param app_id: (str) The BaseSpace ID of the app
        @param config_json: (str) A string encoded json object with the app launch details

        @return (str): the app session ID of the launched app

        @raises AppServicesException if the launch fails
        """
        try:
            # this should return the session ID
            return self._basespace_api.launchApp(app_id, config_json).Id
        except Exception as e:
            raise AppServicesException("App launch failed: %s" % str(e))

    def configure_and_launch_app(self, proto_app):
        """
        configure and launch the app for a particular SampleApp

        @param proto_app: (ProtoApp)

        @return (str): the app session ID of the launched app
        """
        launch_json = self.make_proto_app_launch_payload(proto_app)
        return self.launch_app(proto_app.app.basespaceid, launch_json)

    def simulate_launch(self, sample_app):
        """
        return the app launch template that would be used for a particular SampleApp

        @param sampleApp: (DataAccessLayer.DataAccessLayer._SampleApp)

        @return (str): the app launch json
        """
        return self.make_proto_app_launch_payload(sample_app)

    def get_app_status(self, app_session_id):
        """
        Call BaseSpace to find the status of an app session

        @param app_session_id: (str)

        @return (str): the app status. One of config.PERMITTED_STATUSES

        @raises AppServicesException if the app status from BaseSpace is not recognised
        """
        bs_status = self._basespace_api.getAppSession(app_session_id).Status
        mapping = self._configuration_services.get_config("STATUS_MAPPING")
        try:
            status = mapping[bs_status]
        except KeyError:
            raise AppServicesException("Unknown app session status: %s" % bs_status)
        return status

    def is_app_finished(self, app_session_id):
        return self.get_app_status(app_session_id) == self._configuration_services.get_config("APP_FINISHED_STATUS")

    def get_dependency_file_id(self, app_session_id, app_supplies_details):
        app_result_id = self.app_session_id_to_app_result_id(app_session_id, app_supplies_details.resultname)
        app_result_files = self._basespace_api.getAppResultFiles(app_result_id, self.NO_LIMIT_QP)
        app_file_id = ""
        details_pathglob = app_supplies_details.pathglob
        for app_file in app_result_files:
            if fnmatch.fnmatch(app_file.Name, details_pathglob):
                if app_file_id:
                    raise AppServicesException("Found more than one file matching glob: %s" % details_pathglob)
                app_file_id = app_file.Id
        if app_file_id:
            return app_file_id
        else:
            raise AppServicesException("Could not find file matching glob: %s" % details_pathglob)

    def app_session_id_to_app_result_id(self, app_session_id, resultname=""):
        app_result = self._basespace_api.getAppResultFromAppSessionId(app_session_id, resultname)
        return app_result.Content.Id

    ######
    # Automated QC
    ######

    @staticmethod
    def _compare_qc_result_to_threshold(qc_results, thresholds):
        """
        compare the qcresults from a finished app to some appropriate thresholds
        each threshold entry has a value and an operator. If $(<metric> <operator> <value>) (eg. insert_size ge 300)
        the metric passes qc otherwise it fails

        @param qc_results: (dict) metric_name->value mapping for an app result
        @param thresholds: (dict) metric_name->metric_details.

        @return (list of str): descriptions of the failing metrics

        @raises AppServicesException: if a required metric is missing
        """
        failures = []
        for metricName in thresholds:
            if metricName not in qc_results:
                raise AppServicesException("Metric missing from qc results: %s %s" % (metricName, qc_results))
            threshold_details = thresholds[metricName]
            metric_operator = threshold_details["operator"]
            metric_value = threshold_details["threshold"]
            operator_function = getattr(operator, metric_operator)
            # try/except partly to catch problems with the metrics - eg. NA values
            try:
                if not operator_function(qc_results[metricName], metric_value):
                    failure_message = "%s (%s %s %s)" % (
                        metricName, qc_results[metricName], operator_function.__name__, metric_value)
                    failures.append(failure_message)
            except Exception as e:
                failure_message = "%s (%s %s %s) (%s)" % (
                    metricName, qc_results[metricName], operator_function.__name__, metric_value, str(e))
                failures.append(failure_message)
        return failures

    @staticmethod
    def _read_qc_result(qc_file):
        """
        business logic for reading metrics from a file and packing them into a dictionary
        this business logic is currently selected by file extension and has been tested against the Isaac V2 app
        and the tumour/normal app. This method may need to be extended to support QC for other app types.

        @param qc_file: (filepath)

        @return (dict): metric->value

        @raises AppServicesException: if the metrics file is of unknown type (extension)
        """
        qc_values = {}
        # really crude handling of different file types. Refactor later if desired.
        if qc_file.endswith(".csv"):
            # assumes each row is a key/value pair
            reader = csv.reader(open(qc_file))
            for row in reader:
                if len(row) != 2:
                    continue
                try:
                    qc_values[row[0].strip(":")] = float(row[1].strip("%"))
                except ValueError:
                    continue
        # this assumes a specific format of json based on the tumour/normal output
        # the tumour/normal output has several top-level entries, each of which is a table.
        # this code "flattens" these tables into namespaced elements like
        # VariantStatsTable.Insertions.dbSNP
        elif qc_file.endswith(".json"):
            metrics = json.load(open(qc_file))
            for metricType in metrics:
                metric_details = metrics[metricType]
                if "header" in metric_details:
                    headers = metric_details["header"]
                elif "tableColumns" in metric_details:
                    headers = metric_details["tableColumns"]
                else:
                    continue
                assert "rows" in metric_details, "expected to find rows in metrics details"
                rows = metric_details["rows"]
                for row in rows:
                    for colIndex in range(1, len(headers)):
                        column_name = headers[colIndex]
                        row_name = row[0]
                        row_col_value = row[colIndex]
                        flat_name = "%s.%s.%s" % (metricType, column_name, row_name)
                        qc_values[flat_name] = row_col_value
        else:
            raise AppServicesException("unknown extension on QC file: %s" % qc_file)
        return qc_values

    def apply_automated_qc_to_app_result(self, proto_app):
        """
        Assesses the QC status of an app result from a SampleApp

        @param proto_app: (DataAccessLayer.DataAccessLayer._SampleApp)

        @return (list of str): descriptions of the failing metrics

        @raises AppServicesException: if the app results do not look as expected
        """
        qc_and_delivery = proto_app.app.get_qc_and_delivery()
        if not qc_and_delivery:
            raise AppServicesException("no qc object for app: %s" % proto_app)
        thresholds = qc_and_delivery.get_thresholds_as_dict()
        metrics_file = proto_app.app.metricsfile
        output_dir = proto_app.get_path()
        basespace_appsession_id = proto_app.basespaceid
        app_result_name = proto_app.app.resultname
        qc_dir_name = self._configuration_services.get_config("SAMPLE_LOG_DIR_NAME")
        qc_path = os.path.join(output_dir, qc_dir_name)
        # make directory to write qc file into
        if not os.path.exists(qc_path):
            os.makedirs(qc_path)
        logging.debug("retrieving basespace files with extension %s from appsession Id %s" % (
            metrics_file, basespace_appsession_id))
        qc_files = self._basespace_api.downloadAppResultFilesByExtension(basespace_appsession_id, metrics_file,
                                                                         qc_path,
                                                                         app_result_name,
                                                                         self._no_limit_qp)
        if len(qc_files) != 1:
            raise AppServicesException("did not get exactly one metrics file for QC!")
        qc_file = qc_files[0]
        qc_file_path = os.path.join(qc_path, os.path.basename(qc_file.Path))
        logging.debug("got file: %s" % qc_file_path)
        qc_results = self._read_qc_result(qc_file_path)
        failures = self._compare_qc_result_to_threshold(qc_results, thresholds)
        return failures

    def set_qc_result_in_basespace(self, proto_app, qc_result, details=""):
        """
        uses BaseSpace properties to store the qc result within BaseSpace itself

        https://developer.basespace.illumina.com/docs/content/documentation/rest-api/api-reference#Properties

        @param sample_app: (DataAccessLayer.DataAccessLayer._SampleApp)
        @param qc_result: (bool)
        @param details: (str) why the qc failed

        @raises AppServicesException: if the BaseSpace call fails for any reason
        """
        basespace_id = proto_app.basespaceid
        namespace = self._configuration_services.get_config("QC_NAMESPACE")
        try:
            qc_payload = {"QCResult": str(qc_result)}
            if details:
                qc_payload["QCDetails"] = str(details)
            pr = self._basespace_api.setResourceProperties("appsessions", basespace_id, qc_payload, namespace)
        except Exception as e:
            raise AppServicesException(
                "failed to set QC properties for appsession: %s (%s)" % (basespace_id, str(e)))

    def app_has_qc_conditions(self, app_name):
        app = self._data_access_read.get_app_by_name(app_name)
        if app.get_qc_and_delivery():
            return True
        else:
            return False

    ######
    # Download
    ######

    def download_deliverable(self, proto_app):
        """
        download the configured deliverable file extensions for a given SampleApp

        @param proto_app: (DataAccessLayer.DataAccessLayer._SampleApp)

        @raises AppServicesException: if any parts of the download fail
        """
        output_dir = proto_app.get_path()
        qc_and_delivery = proto_app.app.get_qc_and_delivery()
        if not qc_and_delivery:
            raise AppServicesException("no qc object for app: %s" % proto_app)
        deliverable_list = qc_and_delivery.get_deliverable_as_list()
        basespace_id = proto_app.basespaceid
        app_result_name = proto_app.app.resultname
        for deliverableExtension in deliverable_list:
            logging.info("downloading extension: %s" % deliverableExtension)
            try:
                download_files = self._basespace_api.downloadAppResultFilesByExtension(basespace_id,
                                                                                       deliverableExtension,
                                                                                       output_dir, app_result_name,
                                                                                       self._no_limit_qp)
            except Exception as e:
                raise AppServicesException("failed to download file: %s (%s)" % (deliverableExtension, str(e)))

