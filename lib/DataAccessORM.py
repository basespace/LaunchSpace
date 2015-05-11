import json
import os

__author__ = 'psaffrey'

from peewee import *
import datetime

# this is the one bit of global state that I could not avoid :(
# http://docs.peewee-orm.com/en/0.9.7/peewee/cookbook.html#deferring-initialization
sqlite_database = SqliteDatabase(None)


class BaseModel(Model):
    class Meta:
        database = sqlite_database


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
    type = CharField()
    # json object encoding the app properties
    properties = TextField()
    # json object encoding teh default values for the configuration values for the app
    # these are usually defined as all the non-BaseSpace-entity properties of the app
    defaults = TextField()
    # some apps generate more than one appresult
    # this field specifies which one should be used for QC and download
    resultname = CharField(null=True)
    # extension of the file that stores the metrics for this app
    metricsfile = CharField()
    # json blob with thresholds for the metrics
    qcthresholds = TextField()
    # list of files that constitute a deliverable for this app
    deliverablelist = CharField()
    basespaceid = CharField()

    class Meta:
        indexes = (
            (('name',), True),
        )

    def get_thresholds_as_dict(self):
        return json.loads(self.app.qcthresholds)

    def get_deliverable_as_list(self):
        return self.app.deliverablelist.split(",")

    def get_qc_threshold_summary(self):
        summary = ""
        thresholds = json.loads(self.qcthresholds)
        for tname in thresholds:
            tdetails = thresholds[tname]
            summary += "%s\t%s\t%s\n" % (tname, tdetails["operator"], tdetails["threshold"])
        return summary

    def get_properties_as_dict(self):
        return json.loads(self.properties)

    def get_defaults_as_dict(self):
        return json.loads(self.defaults)

    def __str__(self):
        return """
app: %s (%s)
properties:
%s
defaults:
%s
qc thresholds:
%s
app result name: %s
metrics file: %s
deliverable list: %s
""" % (
        self.name,
        self.basespaceid,
        self.properties,
        self.defaults,
        self.get_qc_threshold_summary(),
        self.resultname,
        self.metricsfile,
        self.deliverablelist
      )


class AppConsumes(BaseModel):
    app = ForeignKeyField(App, on_delete="CASCADE")
    name = CharField()
    type = CharField()
    islistproperty = BooleanField()
    details = CharField()

    class Meta:
        indexes = (
            (('app', 'name'), True)
        )

class AppSupplies(BaseModel):
    app = ForeignKeyField(App, on_delete="CASCADE")
    outputname = CharField()
    # which appresult with an overall app output
    resultname = CharField()
    # how to find the file
    pathglob = CharField()

    class Meta:
        indexes = (
            (('name', ), True)
        )

class SampleApp(BaseModel):
    sample = ForeignKeyField(Sample, on_delete="CASCADE")
    app = ForeignKeyField(App, on_delete="CASCADE")
    basespaceid = CharField(null=True)
    status = CharField()
    statusdetails = TextField(null=True)
    lastupdated = DateTimeField(default=datetime.datetime.now)

    class Meta:
        indexes = (
            (('sample', 'app'), True),
        )

    def __str__(self):
        return self.get_summary(show_details=True)

    def get_path(self):
        outputpath = self.sample.project.outputpath
        app_result_name = "%s.%s" % (self.sample.name, self.app.name)
        return os.path.join(outputpath, app_result_name)

    def get_summary(self, show_details=False):
        if show_details:
            if self.basespaceid:
                return "%s :: %s :: %s (%s) (basespaceid: %s) (%s) (%s)" % (
                    self.app.name, self.sample.project.name, self.sample.name, self.id, self.basespaceid, self.status,
                    self.statusdetails)
            else:
                return "%s :: %s :: %s (%s) (%s) (%s)" % (
                    self.app.name, self.sample.project.name, self.sample.name, self.id, self.status, self.statusdetails)
        else:
            return "%s :: %s :: %s (%s) (%s)" % (
                self.app.name, self.sample.project.name, self.sample.name, self.id, self.status)

    def set_status(self, newstatus, details=""):
        if self.status != newstatus or self.statusdetails != details:
            self.status = newstatus
            self.statusdetails = details
            self.save()

    def set_appsession_id(self, appsession_id):
        self.basespaceid = appsession_id
        self.save()


class SampleRelationship(BaseModel):
    fromsample = ForeignKeyField(Sample, related_name="fromsample", on_delete="CASCADE")
    tosample = ForeignKeyField(Sample, related_name="tosample", on_delete="CASCADE")
    relationship = CharField()

    class Meta:
        indexes = (
            (('fromsample', 'tosample', 'relationship'), True),
        )

# End inner classes
########
