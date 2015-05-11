__author__ = 'psaffrey'

from memoize import memoized
import DataAccessLayer
from DataAccessORM import Sample, Project, App, SampleApp, SampleRelationship, BaseModel
from peewee import DoesNotExist, JOIN_LEFT_OUTER


class DataAccessRead(DataAccessLayer.DataAccessLayer):
    @staticmethod
    def _make_like_string(in_str):
        # these are *s and not %s because peewee uses GLOB instead of LIKE (for sqlite)
        # so you need a different wildcard operator
        return "*%s*" % in_str

    def _augment_query(self, query, match_object, query_term, exact):
        """
        augment a peewee query object with an additional condition

        http://peewee.readthedocs.org/en/latest/peewee/querying.html#selecting-multiple-records
        """
        if isinstance(query_term, list):
            query = query.where(match_object << query_term)
        else:
            if exact:
                query = query.where(match_object == query_term)
            else:
                query = query.where(match_object % self._make_like_string(query_term))
        return query

    def get_all_projects(self):
        return [project for project in Project.select()]

    def get_all_samples(self):
        return [sample for sample in Sample.select()]

    def get_all_samples_with_relationships(self):
        # the big select and join here is important,
        # because it means all the foreign key relationships are prepacked into the returned peewee objects
        # otherwise, peewee will make separate queries to resolve foreign key connections
        return [sample for sample in
                Sample.select(Sample, SampleRelationship).join(SampleRelationship,
                                                               JOIN_LEFT_OUTER)]

    def has_sample(self, sample_name):
        try:
            self.get_sample_by_name(sample_name)
            return True
        except DataAccessLayer.DBMissingException:
            return False

    def get_all_apps(self):
        return [app for app in App.select()]

    def has_app(self, app_name):
        try:
            self.get_app_by_name(app_name)
            return True
        except DataAccessLayer.DBMissingException:
            return False

    def get_sample_app_mapping(self):
        # we need to join here to convert IDs to names.
        # the peewee syntax is pretty gnarly. Hopefully it makes the query efficient
        # http://peewee.readthedocs.org/en/latest/peewee/querying.html#joining-on-multiple-tables
        query = SampleApp.select().join(Sample).switch(SampleApp).join(App)
        return [(row.sample.name, row.app.name) for row in query]

    def get_sample_app_by_id(self, sample_app_id):
        try:
            return SampleApp.get(SampleApp.id == sample_app_id)
        except DoesNotExist:
            raise DataAccessLayer.DBMissingException("missing SampleApp: %s" % sample_app_id)

    def get_sample_apps_by_constraints(self, constraints, exact=False):
        # if we've selected by a particular ID, we don't need to check the other constraints
        if "id" in constraints:
            return [self.get_sample_app_by_id(constraints["id"])]
        # if we join here then it pulls down all the foreign key connections into the objects
        # this prevents excessive object dereference queries occuring in any downstream code
        # the peewee syntax is pretty gnarly. Hopefully it makes the query efficient
        # http://peewee.readthedocs.org/en/latest/peewee/querying.html#joining-on-multiple-tables
        query = (SampleApp.select(Sample, Project, SampleApp, App)
                 .join(Sample)
                 .join(Project)
                 .switch(SampleApp)
                 .join(App))
        # a fair amount of repetition here
        # but I decided I preferred this to a more convoluted generic mechanism
        if "project" in constraints:
            query_field = Project.name
            query = self._augment_query(query, query_field, constraints["project"], exact)
        if "sample" in constraints:
            query_field = Sample.name
            query = self._augment_query(query, query_field, constraints["sample"], exact)
        if "status" in constraints:
            query_field = SampleApp.status
            query = self._augment_query(query, query_field, constraints["status"], exact)
        if "type" in constraints:
            query_field = App.type
            query = self._augment_query(query, query_field, constraints["type"], exact)
        if "name" in constraints:
            query_field = App.name
            query = self._augment_query(query, query_field, constraints["name"], exact)

        return [x for x in query]

    @staticmethod
    def get_sample_relationships(sample):
        try:
            sr = sample.samplerelationship
            # if there is not relationship, this will throw an exception
            fs = sr.fromsample
            return sr
        except:
            raise DataAccessLayer.DBMissingException("sample relationship could not be found")

    def get_normal_from_tumour(self, sample_name):
        sample = self.get_sample_by_name(sample_name)
        sr = SampleRelationship.get(
            SampleRelationship.fromsample == sample)
        assert sr.relationship == "TumourNormal"
        return sr.tosample

    def get_sample_summary(self, sample):
        summary = [sample.name, sample.project.name]
        try:
            sr = self.get_sample_relationships(sample)
            summary.extend([sr.relationship, sr.tosample.name])
        except DataAccessLayer.DBMissingException:
            pass
        return "\t".join(summary)

    def print_sample_summaries(self, samples):
        with self.database.transaction():
            for sample in samples:
                print self.get_sample_summary(sample)