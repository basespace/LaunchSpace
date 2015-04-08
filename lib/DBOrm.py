"""
Peewee ORM object definitions

http://peewee.readthedocs.org/en/latest/
"""

from peewee import *

import datetime

import os
import sys

# Add relative path libraries
SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.append(os.path.abspath(os.path.sep.join([SCRIPT_DIR, "..", "lib"])))

import ConfigurationServices
DBFile = ConfigurationServices.GetConfig("DBFile")
database = SqliteDatabase(DBFile)


UPDATE_TRIGGER = """create trigger set_lastupdated after update on SampleApp
 begin
    update SampleApp set lastupdated = datetime('NOW') where id = new.id;
end;"""


def create_tables():
    """
    sets up the database froms scratch based on the peewee objects
    called by InstantiateDatabase.py
    """
    print "instantiating into file: %s" % DBFile
    database.connect()
    database.create_tables([Sample, Project, App, SampleApp, SampleRelationship])
    cursor = database.get_cursor()
    print "adding update trigger..."
    cursor.execute(UPDATE_TRIGGER)
    database.close()


# def before_request_handler():
#     database.connect()

# def after_request_handler():
#     database.close()

class BaseModel(Model):
    class Meta:
        database = database

class Project(BaseModel):
    name = CharField()
    outputpath = CharField()
    basespaceid = CharField()

    class Meta:
        indexes = (
            (('name',), True),
        )


class Sample(BaseModel):
    name = CharField()
    project = ForeignKeyField(Project, on_delete="CASCADE")
    created = DateTimeField(default=datetime.datetime.now)

    class Meta:
        indexes = (
            (('name',), True),
        )

class App(BaseModel):
    name = CharField()
    type = CharField()
    template = TextField()
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

class SampleRelationship(BaseModel):
    fromsample = ForeignKeyField(Sample, related_name="fromsample", on_delete="CASCADE")
    tosample = ForeignKeyField(Sample, related_name="tosample", on_delete="CASCADE")
    relationship = CharField()

    class Meta:
        indexes = (
            (('fromsample', 'tosample', 'relationship'), True),
        )
