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
from LaunchSpecification import LaunchSpecification


class AppServicesException(Exception):
    pass


class AppServices(object):
    def __init__(self, basespace_api, sample_services, configuration_services, data_access_read):
        self._basespace_api = basespace_api
        self._no_limit_qp = QueryParameters({"Limit": 1000})
        self._sample_services = sample_services
        self._configuration_services = configuration_services
        self._data_access_read = data_access_read

    ######
    # routines to check conditions to determine whether SampleApps are ready for launch
    ######

    def check_conditions_on_sample_app(self, sample_app, ignore_yield=False):
        """
        Evaluate whether a SampleApp is ready to be launched
        this is dependent on the app type ("SingleGenome", or "TumourNormal")

        @param sample_app: (DataAccessLayer.DataAccessLayer._SampleApp)
        @param ignore_yield: (bool)

        @return (bool): whether the conditions are met, (str): any details about why conditions are not met
        """
        # unpack some stuff from the objects
        project_id = sample_app.sample.project.basespaceid
        sample_name = sample_app.sample.name
        app_type = sample_app.app.type
        if app_type == "SingleGenome":
            # if this is just a build, just check this sample is ready to go
            return self._sample_services.check_conditions_on_sample(self, sample_name, project_id, ignore_yield)
        if app_type == "TumourNormal":
            # if this is a subtraction, look up the two samples and check the readiness of both
            tumour_name = sample_name
            tumour_ready, tumour_details = self._sample_services.check_conditions_on_sample(tumour_name, project_id,
                                                                                            ignore_yield)
            if not tumour_ready:
                return False, "(Tumour: %s)" % tumour_details
            normal = self._data_access_read.get_tumour_for_normal(sample_name)
            normal_name = normal.name
            normal_ready, normal_details = self._sample_services.check_conditions_on_sample(normal_name, project_id,
                                                                                            ignore_yield)
            if not normal_ready:
                return False, "(Normal: %s)" % normal_details
            return True, None

    ######
    # app launch and tracking
    ######

    # this is the part that we're going to replace when we make the dependencies more generic
    # 1. Get all the dependencies for the app
    # 2. resolve them based on their property name

    def get_app_launch_properties(self, sample_app):
        app_type = sample_app.app.type
        launch_properties = dict()
        launch_properties["project-id"] = sample_app.sample.project.basespaceid
        sample_name = sample_app.sample.name
        project_id = sample_app.sample.project.basespaceid
        sample_basespace_id = self._sample_services.get_basespace_sample_id(sample_name, project_id)
        if not sample_basespace_id:
            raise AppServicesException("no BaseSpace sample: %s" % sample_name)
        if app_type == "SingleGenome":
            launch_properties["sample-id"] = sample_basespace_id
        elif app_type == "TumourNormal":
            tumour_sample = sample_app.sample
            normal_sample = self._data_access_read.get_normal_from_tumour(tumour_sample)
            normal_sample_name = normal_sample.name
            normal_sample_basespace_id = self._sample_services.get_basespace_sample_id(normal_sample_name, project_id)
            launch_properties["tumor-sample-id"] = sample_basespace_id
            launch_properties["sample-id"] = normal_sample_basespace_id
        else:
            raise AppServicesException("Unsupported app type: %s" % app_type)
        return launch_properties

    def make_sample_app_launch_payload(self, sample_app):
        launch_specification = LaunchSpecification(sample_app.app.get_properties_as_dict(),
                                                   sample_app.app.get_defaults_as_dict(),
                                                   self._configuration_services)
        launch_properties = self.get_app_launch_properties(sample_app)
        # I'm going to have to make the whole thing more generic for ProtoApps anyway
        launch_name = "%s : %s" % (sample_app.sample.name, sample_app.app.name)
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

    def configure_and_launch_app(self, sample_app):
        """
        configure and launch the app for a particular SampleApp

        @param sample_app: (DataAccessLayer.DataAccessLayer._SampleApp)

        @return (str): the app session ID of the launched app
        """
        launch_json = self.make_sample_app_launch_payload(sample_app)
        return self.launch_app(sample_app.app.basespaceid, launch_json)

    def simulate_launch(self, sample_app):
        """
        return the app launch template that would be used for a particular SampleApp

        @param sampleApp: (DataAccessLayer.DataAccessLayer._SampleApp)

        @return (str): the app launch json
        """
        return self.make_sample_app_launch_payload(sample_app)

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

    def _read_qc_result(self, qc_file):
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

    def apply_automated_qc_to_app_result(self, sample_app):
        """
        Assesses the QC status of an app result from a SampleApp

        @param sample_app: (DataAccessLayer.DataAccessLayer._SampleApp)

        @return (list of str): descriptions of the failing metrics

        @raises AppServicesException: if the app results do not look as expected
        """
        thresholds = sample_app.get_thresholds_as_dict()
        metrics_file = sample_app.app.metricsfile
        output_dir = sample_app.get_path()
        basespace_appsession_id = sample_app.basespaceid
        app_result_name = sample_app.app.resultname
        qc_dir_name = self._configuration_services.get_config("SAMPLE_LOG_DIR_NAME")
        qc_path = os.path.join(output_dir, qc_dir_name)
        # make directory to write qc file into
        if not os.path.exists(qc_path):
            os.makedirs(qc_path)
        logging.debug("retrieving basespace files with extension %s from appsession Id %s" % (
            metrics_file, basespace_appsession_id))
        qc_files = self._basespace_api.downloadAppResultFilesByExtension(basespace_appsession_id, metrics_file, qc_path,
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

    def set_qc_result_in_basespace(self, sample_app, qc_result, details=""):
        """
        uses BaseSpace properties to store the qc result within BaseSpace itself

        https://developer.basespace.illumina.com/docs/content/documentation/rest-api/api-reference#Properties

        @param sample_app: (DataAccessLayer.DataAccessLayer._SampleApp)
        @param qc_result: (bool)
        @param details: (str) why the qc failed

        @raises AppServicesException: if the BaseSpace call fails for any reason
        """
        basespace_id = sample_app.basespaceid
        namespace = self._configuration_services.get_config("QC_NAMESPACE")
        try:
            qc_payload = {"QCResult": str(qc_result)}
            if details:
                qc_payload["QCDetails"] = str(details)
            pr = self._basespace_api.setResourceProperties("appsessions", basespace_id, qc_payload, namespace)
        except Exception as e:
            raise AppServicesException("failed to set QC properties for appsession: %s (%s)" % (basespace_id, str(e)))

    ######
    # Download
    ######

    def download_deliverable(self, sample_app):
        """
        download the configured deliverable file extensions for a given SampleApp

        @param sample_app: (DataAccessLayer.DataAccessLayer._SampleApp)

        @raises AppServicesException: if any parts of the download fail
        """
        output_dir = sample_app.get_path()
        deliverable_list = sample_app.app.get_deliverable_as_list()
        basespace_id = sample_app.basespaceid
        app_result_name = sample_app.app.resultname
        for deliverableExtension in deliverable_list:
            logging.info("downloading extension: %s" % deliverableExtension)
            try:
                download_files = self._basespace_api.downloadAppResultFilesByExtension(basespace_id,
                                                                                       deliverableExtension,
                                                                                       output_dir, app_result_name,
                                                                                       self._no_limit_qp)
            except Exception as e:
                raise AppServicesException("failed to download file: %s (%s)" % (deliverableExtension, str(e)))

