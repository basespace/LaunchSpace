import json
import os

__author__ = 'psaffrey'

from peewee import *
from flask_peewee.utils import get_dictionary_from_model
import datetime
import re

# this is the one bit of global state that I could not avoid :(
# http://docs.peewee-orm.com/en/0.9.7/peewee/cookbook.html#deferring-initialization
sqlite_database = SqliteDatabase(None)


class BaseModel(Model):
    class Meta:
        database = sqlite_database

    def to_dict(self):
        return get_dictionary_from_model(self)


class Project(BaseModel):
    name = CharField()
    outputpath = CharField()
    basespaceid = CharField()

    class Meta:
        indexes = (
            (('name',), True),
        )

    def __str__(self):
        return "\t".join([self.name, self.outputpath, self.basespaceid])


class Sample(BaseModel):
    name = CharField()
    project = ForeignKeyField(Project, on_delete="CASCADE")
    created = DateTimeField(default=datetime.datetime.now)

    class Meta:
        indexes = (
            (('name',), True),
        )

    def __str__(self):
        return self.name


class App(BaseModel):
    name = CharField()
    # json object encoding the app properties
    properties = TextField()
    # json object encoding teh default values for the configuration values for the app
    # these are usually defined as all the non-BaseSpace-entity properties of the app
    defaults = TextField()
    basespaceid = CharField()

    class Meta:
        indexes = (
            (('name',), True),
        )

    def get_flat_name(self):
        """
        turn app name into valid Python identifier
        http://stackoverflow.com/a/3303361
        """
        s = self.name
        # Remove invalid characters
        s = re.sub('[^0-9a-zA-Z_]', '', s)
        # Remove leading characters until we find a letter or underscore
        s = re.sub('^[^a-zA-Z_]+', '', s)
        return s

    def get_properties_as_dict(self):
        return json.loads(self.properties)

    def get_defaults_as_dict(self):
        return json.loads(self.defaults)

    def get_inputs(self):
        query = AppConsumes.select().where(AppConsumes.app == self)
        return [x for x in query]

    def get_input_by_name(self, parameter):
        # get all the inputs that match the parameter name - there should be only 1!
        parameter_inputs = [input_ for input_ in self.get_inputs() if input_.name == parameter]
        assert len(parameter_inputs) == 1
        return parameter_inputs[0]

    def get_outputs(self):
        query = AppSupplies.select().where(AppSupplies.app == self)
        return [x for x in query]

    def get_output_by_name(self, output_name):
        # get all the outputs name - there should be only 1!
        outputs = [output_ for output_ in self.get_outputs() if output_.outputname == output_name]
        if not outputs:
            return None
        assert len(outputs) == 1
        return outputs[0]

    def get_sample_dependency_name(self):
        """
        Finds any sample dependency and returns it. Enforces a single dependency
        """
        properties = self.get_properties_as_dict()
        sample_properties = [property_ for property_ in properties if property_["Type"] == "Sample"]
        assert len(sample_properties) == 1
        return sample_properties[0]

    def get_qc_and_delivery(self):
        try:
            return AppQCAndDelivery.get(AppQCAndDelivery.app == self)
        except:
            return None

    def __str__(self):
        reported_parts = ["app: %s (%s)" % (self.name, self.basespaceid),
                          "properties: %s" % str(self.properties),
                          "defaults: %s" % str(self.defaults)]
        inputs = self.get_inputs()
        outputs = self.get_outputs()
        if inputs:
            flat_inputs = ",".join([str(x) for x in inputs])
            reported_parts.append("consumes: %s" % flat_inputs)
        if outputs:
            flat_outputs = ",".join([str(x) for x in outputs])
            reported_parts.append("produces: %s" % flat_outputs)
        return "\n".join(reported_parts)


class AppQCAndDelivery(BaseModel):
    app = ForeignKeyField(App, on_delete="CASCADE")
    # some apps generate more than one appresult
    # this field specifies which one should be used for QC and download
    resultname = CharField(null=True)
    # extension of the file that stores the metrics for this app
    metricsfile = CharField()
    # json blob with thresholds for the metrics
    qcthresholds = TextField()
    # list of files that constitute a deliverable for this app
    deliverablelist = CharField()

    def get_thresholds_as_dict(self):
        return json.loads(self.qcthresholds)

    def get_deliverable_as_list(self):
        return self.deliverablelist.split(",")

    def get_qc_threshold_summary(self):
        summary = ""
        thresholds = json.loads(self.qcthresholds)
        for tname in thresholds:
            tdetails = thresholds[tname]
            summary += "%s\t%s\t%s\n" % (tname, tdetails["operator"], tdetails["threshold"])
        return summary

        # this doesn't seem to work, I think because it's a foreign key
        # class Meta:
        #     indexes = (
        #         (('app', ), True),
        #     )


class AppConsumes(BaseModel):
    app = ForeignKeyField(App, on_delete="CASCADE")
    name = CharField()
    type = CharField()
    islistproperty = BooleanField()
    description = CharField(null=True)

    class Meta:
        indexes = (
            (('app', 'name'), True),
        )

    def __str__(self):
        if self.description:
            return "%s (%s: %s)" % (self.name, self.type, self.description)
        else:
            return "%s (%s)" % (self.name, self.type)


class AppSupplies(BaseModel):
    app = ForeignKeyField(App, on_delete="CASCADE")
    outputname = CharField()
    # which appresult within an overall app output
    resultname = CharField()
    # extra information about the output such as "vcf"
    # this will be compared against downstream inputs ot make sure they match
    type = CharField()
    # how to find the file
    pathglob = CharField()

    class Meta:
        indexes = (
            (('outputname', ), True),
        )

    def __str__(self):
        if self.type:
            return "%s (%s: %s)" % (self.outputname, self.pathglob, self.type)
        else:
            return "%s (%s)" % (self.outputname, self.pathglob)


class ProtoAppOutputDescription(object):
    def __init__(self, name, owner, type_):
        self.name = name
        self.owner = owner
        self.type = type_


class ProtoApp(BaseModel):
    app = ForeignKeyField(App, on_delete="CASCADE")
    project = ForeignKeyField(Project, on_delete="CASCADE")
    basespaceid = CharField(null=True)
    status = CharField()
    statusdetails = TextField(null=True)
    lastupdated = DateTimeField(default=datetime.datetime.now)
    outputs = {}

    def __str__(self):
        return self.get_summary(show_details=True)

    @staticmethod
    def condense_sample_list(sample_list):
        if len(sample_list) < 3:
            return "+".join(sample_list)
        else:
            return "%s+ETC" % sample_list[0]

    def initialise_outputs(self):
        """
        from the app definition, set up objects that will allow the workflow definer
        to get hold of information about the outputs of this protoapp
        """
        # get all the things apps of this type makes; use these to initialise the outputs
        app_outputs = self.get_app_outputs()
        for output in app_outputs:
            output_name = output.outputname
            output_type = output.type
            self.outputs[output_name] = ProtoAppOutputDescription(output_name, self, output_type)

    def get_app_outputs(self):
        query = AppSupplies.select().where(AppSupplies.app == self.app)
        return query

    def get_distinctive_name(self):
        proto_app_dependencies = self.get_dependencies()
        sample_dependencies = [dependency.sampledependency.name for dependency in proto_app_dependencies if
                               dependency.sampledependency]
        if sample_dependencies:
            app_result_name = "%s.%s" % (self.condense_sample_list(sample_dependencies), self.app.get_flat_name())
        else:
            app_result_name = "%s_%s" % (self.app.get_flat_name(), self.id)
        return app_result_name

    def get_path(self):
        projectpath = self.project.outputpath
        app_result_name = self.get_distinctive_name()
        return os.path.join(projectpath, app_result_name)

    def get_summary(self, show_details=False):
        if show_details:
            if self.basespaceid:
                return "%s :: (%s) (basespaceid: %s) (%s) (%s)" % (
                    self.app.name, self.id, self.basespaceid, self.status,
                    self.statusdetails)
            else:
                return "%s :: (%s) (%s) (%s)" % (
                    self.app.name, self.id, self.status, self.statusdetails)
        else:
            return "%s :: (%s) (%s)" % (
                self.app.name, self.id, self.status)

    def set_status(self, newstatus, details=""):
        if self.status != newstatus or self.statusdetails != details:
            self.status = newstatus
            self.statusdetails = details
            self.save()

    def set_appsession_id(self, appsession_id):
        self.basespaceid = appsession_id
        self.save()

    def get_dependencies(self):
        app_dependency_query = ProtoAppDependency.select().where(ProtoAppDependency.dependent == self)
        return [x for x in app_dependency_query]


class ProtoAppDependency(BaseModel):
    # this is the protoapp that depends on something
    dependent = ForeignKeyField(ProtoApp, on_delete="CASCADE")
    parametername = CharField()
    # these mutually exclusive fields describes what the protoapp depends on - either a sample or another protoapp
    # one of these will be null but not the other!
    sampledependency = ForeignKeyField(Sample, related_name="sampledependency", null=True)
    protoappdependency = ForeignKeyField(ProtoApp, related_name="protoappdependency", null=True)
    # if this ProtoApp depends on another ProtoApp, we may need this field to describe which of the dependency's
    # fields we want, which will be resolved against the relevant AppSupplies outputname field
    # this is used by the get_app_supplies method below
    description = CharField(null=True)

    def get_app_supplies(self):
        """
        get the AppSupplies object which has more details about how to find the relevant dependency information
        note that this is the AppSupplies for the ProtoApp we depend on, not the one that has the dependency
        """
        try:
            return AppSupplies.get(
                AppSupplies.app == self.protoappdependency.app and AppSupplies.outputname == self.description
            )
        except:
            return None

    def get_app_consumes(self):
        """
        get the AppConsumes object which has more details about what we are expecting to consume
        note that this is the AppConsumes object for the ProtoApp that has the dependency
        """
        return AppConsumes.get(
            AppConsumes.app == self.dependent and AppConsumes.name == self.parametername
        )

    def __str__(self):
        if self.sampledependency:
            return "sample:%s" % (self.sampledependency)
        elif self.protoappdependency:
            if self.description:
                return "protoapp: %s (%s)" % (self.protoappdependency.get_distinctive_name(), self.description)
            else:
                return "protoapp: %s" % (self.protoappdependency.get_distinctive_name())
        else:
            return ""


class SampleRelationship(BaseModel):
    fromsample = ForeignKeyField(Sample, related_name="fromsample", on_delete="CASCADE")
    tosample = ForeignKeyField(Sample, related_name="tosample", on_delete="CASCADE")
    relationship = CharField()

    class Meta:
        indexes = (
            (('fromsample', 'tosample', 'relationship'), True),
        )

