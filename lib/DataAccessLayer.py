import json
import os

__author__ = 'psaffrey'

"""
ORM definitions and then a DAL class to wrap access to them

This is a base class on which other DataAccessLayer classes derive.
This base class should not be instantiated directly.

Currently only supports SQLite databases - it takes a file to build the proper objects
"""

from peewee import *
import datetime

from DataAccessORM import Sample, Project, App, SampleApp, SampleRelationship, BaseModel, sqlite_database


class DBException(Exception):
    pass


class DBExistsException(DBException):
    pass


class DBMissingException(DBException):
    pass


class DBFormatException(DBException):
    pass


class DataAccessLayer(object):
    UPDATE_TRIGGER = """create trigger set_lastupdated after update on SampleApp
     begin
        update SampleApp set lastupdated = datetime('NOW') where id = new.id;
    end;"""

    DEFAULT_STATUS = ""
    PERMITTED_STATUSES = set()

    def __init__(self, database_path, cs):
        self._database_path = database_path
        self._configuration_service = cs
        # initialise database from ORM database global variable
        self.database = sqlite_database
        self.database.init(self._database_path)
        self.DEFAULT_STATUS = cs.get_config("DEFAULT_STATUS")
        self.PERMITTED_STATUSES = cs.get_config("PERMITTED_STATUSES")

    def create_tables(self):
        """
        sets up the database froms scratch based on the peewee objects
        called by InstantiateDatabase.py
        """
        print "instantiating into file: %s" % self._database_path
        self.database.connect()
        self.database.create_tables(
            [Sample, Project, App, SampleApp, SampleRelationship])
        cursor = self.database.get_cursor()
        print "adding update trigger..."
        cursor.execute(self.UPDATE_TRIGGER)
        self.database.close()

    #######
    # Basic GET methods which everybody needs

    @staticmethod
    def get_project_by_name(project_name):
        """

        :rtype : DataAccessLayer._Project
        """
        try:
            return Project.get(Project.name == project_name)
        except DoesNotExist:
            raise DBMissingException("missing project: %s" % project_name)

    @staticmethod
    def get_sample_by_name(sample_name):
        try:
            return Sample.get(Sample.name == sample_name)
        except DoesNotExist:
            raise DBMissingException("missing sample: %s" % sample_name)

    @staticmethod
    def get_app_by_name(app_name):
        try:
            return App.get(App.name == app_name)
        except DoesNotExist:
            raise DBMissingException("missing app: %s" % app_name)

