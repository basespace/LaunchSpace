import csv

__author__ = 'psaffrey'

import DataAccessLayer
from DataAccessORM import Sample, Project, App, AppQCAndDelivery, ProtoApp, ProtoAppDependency, SampleRelationship, \
    AppSupplies, AppConsumes
from peewee import IntegrityError
from LaunchSpecification import LaunchSpecification


class DataAccessCreate(DataAccessLayer.DataAccessLayer):
    #######
    # Projects
    #######

    @staticmethod
    def add_project(project_name, output_path, basespace_id):
        try:
            return Project.create(
                name=project_name,
                outputpath=output_path,
                basespaceid=basespace_id
            )
        except IntegrityError:
            raise DataAccessLayer.DBExistsException("project already exists!")

    #######
    # App-related
    #######

    def add_apps_from_blob(self, app_blob):
        for app in app_blob:
            app_obj = self.add_app(
                app_name=app["name"],
                app_properties=app["properties"],
                app_defaults=app["defaults"],
                input_details=app["inputdetails"],
                basespace_id=app["basespaceid"]
            )
            if "outputs" in app:
                self.add_app_output_description(
                    app=app_obj,
                    output_name=outputs["outputname"], 
                    output_type=outputs["outputtype"],
                    result_name=outputs["resultname"], 
                    path_glob=outputs["pathglob"]
                    )


    def add_app(self, app_name, app_properties, app_defaults, input_details, basespace_id):
        try:
            app = App.create(
                name=app_name,
                properties=app_properties,
                defaults=app_defaults,
                basespaceid=basespace_id
            )
        except IntegrityError as e:
            print str(e)
            raise DataAccessLayer.DBExistsException("app already exists!")
        properties = app.get_properties_as_dict()
        defaults = app.get_defaults_as_dict()
        launch_specification = LaunchSpecification(properties, defaults, self._configuration_service)
        parameters = launch_specification.get_minimum_requirements()
        for parameter_name in parameters:
            # the project is implicit!
            if parameter_name == "project-id":
                continue
            parameter_type = launch_specification.get_property_bald_type(parameter_name)
            is_list_property = launch_specification.is_list_property(parameter_name)
            if parameter_name in input_details:
                parameter_details = input_details[parameter_name]
                self.add_app_input_description(app, parameter_name, parameter_type, is_list_property,
                                               parameter_details)
            else:
                self.add_app_input_description(app, parameter_name, parameter_type,
                                               is_list_property=is_list_property)
        return app

    @staticmethod
    def add_app_qc_and_delivery(app, app_result_name, metrics_file, qc_thresholds, deliverable_list):
        try:
            qc_and_delivery = AppQCAndDelivery.create(
                app=app,
                resultname=app_result_name,
                metricsfile=metrics_file,
                qcthresholds=qc_thresholds,
                deliverablelist=deliverable_list
            )
        except IntegrityError:
            raise DataAccessLayer.DBExistsException("QC and delivery already exists for App: %s" % app)

    @staticmethod
    def add_app_output_description(app, output_name, output_type, result_name, path_glob):
        try:
            return AppSupplies.create(
                app=app,
                outputname=output_name,
                type=output_type,
                resultname=result_name,
                pathglob=path_glob
            )
        except IntegrityError:
            raise DataAccessLayer.DBExistsException("app output name already exists: %s" % output_name)

    @staticmethod
    def update_app_output_description(app, output_name, output_type, result_name, path_glob):
        try:
            output_ = app.get_output_by_name(output_name)
            output_.type = output_type
            output_.resultname = result_name
            output_.pathglob = path_glob
            output_.save()
        except:
            raise


    @staticmethod
    def add_app_input_description(app, input_name, input_type, is_list_property, details=""):
        try:
            return AppConsumes.create(
                app=app,
                name=input_name,
                type=input_type,
                islistproperty=is_list_property,
                details=details
            )
        except IntegrityError as e:
            print str(e)
            raise DataAccessLayer.DBExistsException("app input name already exists: %s" % input_name)

    @staticmethod
    def update_app_input_description(app, parameter_name, description):
        try:
            input_ = app.get_input_by_name(parameter_name)
            input_.description = description
            input_.save()
        except:
            raise

    #######
    # ProtoApp related
    #######

    def add_proto_app(self, app, project, status=""):
        # workaround for the fact that I can't use self in a default argument
        if not status:
            status = self.DEFAULT_STATUS
        assert status in self.PERMITTED_STATUSES, "bad status: %s" % status

        try:
            return ProtoApp.create(
                app=app,
                project=project,
                status=status,
            )
        # this means that this sample/app pairing already exists
        except IntegrityError as e:
            pass

    # shortcut for adding a protoapp and then adding a sample dependency
    def add_sample_app(self, sample_name, project_name, app_name):
        try:
            app = self.get_app_by_name(app_name)
            sample = self.get_sample_by_name(sample_name)
            project = self.get_project_by_name(project_name)
        except DataAccessLayer.DBMissingException:
            raise

        pa = self.add_proto_app(app, project_name)
        sample_dependency_name = app.get_sample_dependency_name()
        self.add_proto_app_dependency(pa, sample_dependency_name, sample, None, None)

    @staticmethod
    def add_proto_app_dependency(proto_app, parameter_name, sample_dependency, protoapp_dependency, description):
        try:
            return ProtoAppDependency.create(
                dependent=proto_app,
                parametername=parameter_name,
                sampledependency=sample_dependency,
                protoappdependency=protoapp_dependency,
                description=description
            )
        except Exception as e:
            raise

    #######
    # Sample-related
    #######

    def add_sample(self, sample_name, project_name):
        """

        :rtype : Sample
        """
        try:
            project = self.get_project_by_name(project_name)
        except DataAccessLayer.DBMissingException:
            raise

        try:
            return Sample.create(
                name=sample_name,
                project=project
            )
        # this means that this sample already exists
        except IntegrityError:
            return None

    @staticmethod
    def add_sample_relationship(from_sample, to_sample, relationship_name):
        """

        :rtype : SampleRelationship
        """
        try:
            return SampleRelationship.create(
                fromsample=from_sample,
                tosample=to_sample,
                relationship=relationship_name
            )
        except DataAccessLayer.DBMissingException:
            raise
        # this means that this relationship already exists
        except IntegrityError:
            return None

    # it might be better to have this method as a separate class
    def add_samples_from_lims_file(self, project_name, lims_file, TN_RELATIONSHIP_NAME):

        # generator function to strip the extraneous stuff from around a LIMS manifest
        # this means we can pass it straight into csv.DictReader
        def read_file_strip_header(infile):
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

        HEADER_TAG = "TABLE HEADER"
        DATA_TAG = "SAMPLE ENTRIES"
        SAMPLE_HEADER = "Sample/Name"
        MATCHED_HEADER = "UDF/Match Sample IDs"
        IS_TUMOUR_HEADER = "UDF/Is Tumor Sample"
        ANALYSIS_HEADER = "UDF/Analysis"
        #TN_RELATIONSHIP_NAME = ConfigurationServices.GetConfig("TN_RELATIONSHIP_NAME")
        project = self.get_project_by_name(project_name)
        dr = csv.DictReader(read_file_strip_header(lims_file), delimiter="\t")
        samples = set()
        relationships = set()
        with self.database.transaction():
            for row in dr:
                sample_name = row[SAMPLE_HEADER]
                pair_name = row[MATCHED_HEADER]
                is_tumour = row[IS_TUMOUR_HEADER]
                app_name = row[ANALYSIS_HEADER]
                sample1 = self.add_sample(sample_name, project)
                sample2 = self.add_sample(pair_name, project)
                if sample1:
                    samples.add(sample1)
                if sample2:
                    samples.add(sample2)
                    # the tumour sample owns the analysis!
                if is_tumour == "TRUE":
                    self.add_sample_app(sample_name, project_name, app_name)
                    relationship = self.add_sample_relationship(
                        from_sample=sample1,
                        to_sample=sample2,
                        relationship=TN_RELATIONSHIP_NAME
                    )
                    if relationship:
                        relationships.add(relationship)
        return samples, relationships

    def configure_samples_from_file(self, project_name, in_file):
        try:
            project = self.get_project_by_name(project_name)
        except DataAccessLayer.DBMissingException:
            raise
        samples = set()
        relationships = set()
        with self.database.transaction():
            reader = csv.reader(open(in_file), delimiter="\t")
            relationships_to_make = []
            for row in reader:
                if len(row) == 4:
                    sample_name, app_name, related_sample, relationship = row
                    # gather up this relationship - we'll deal with it at the end
                    relationships_to_make.append((sample_name, related_sample, relationship))
                elif len(row) == 2:
                    sample_name, app_name = row
                else:
                    raise DataAccessLayer.DBFormatException("wrong number of columns in sample input file: %s" % row)
                sample = self.add_sample(
                    sample_name=sample_name,
                    project=project
                )
                if sample:
                    samples.add(sample)
                self.add_sample_app(sample_name, project_name, app_name)
            for relationship in relationships_to_make:
                from_sample, to_sample, relationship_name = relationship
                # the first in the relationship must already have an entry from the loop above
                # but the second may not
                sample = self.add_sample(
                    sample_name=to_sample,
                    project=project
                )
                if sample:
                    samples.add(sample)
                relationship = self.add_sample_relationship(
                    from_sample=from_sample,
                    to_sample=to_sample,
                    relationship=relationship_name
                )
                relationships.add(relationship)
        return samples, relationships