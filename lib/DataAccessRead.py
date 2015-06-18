__author__ = 'psaffrey'

from memoize import memoized
import DataAccessLayer
from DataAccessORM import Sample, Project, App, ProtoApp, ProtoAppDependency, SampleRelationship, BaseModel
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

    @staticmethod
    def get_all_projects():
        return [project for project in Project.select()]

    @staticmethod
    def get_all_samples():
        return [sample for sample in Sample.select()]

    @staticmethod
    def get_all_samples_with_relationships():
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

    @staticmethod
    def get_all_apps():
        return [app for app in App.select()]

    def get_all_app_names(self):
        return [x.name for x in self.get_all_apps()]

    def has_app(self, app_name):
        try:
            self.get_app_by_name(app_name)
            return True
        except DataAccessLayer.DBMissingException:
            return False

    @staticmethod
    def get_proto_app_by_id(proto_app_id):
        try:
            return ProtoApp.get(ProtoApp.id == proto_app_id)
        except DoesNotExist:
            raise DataAccessLayer.DBMissingException("missing ProtoApp: %s" % proto_app_id)

    def get_proto_apps_by_constraints(self, constraints, exact=False):
        # if we've selected by a particular ID, we don't need to check the other constraints
        if "id" in constraints:
            return [self.get_proto_app_by_id(constraints["id"])]
        # if we join here then it pulls down all the foreign key connections into the objects
        # this prevents excessive object dereference queries occuring in any downstream code
        # the peewee syntax is pretty gnarly. Hopefully it makes the query efficient
        # http://peewee.readthedocs.org/en/latest/peewee/querying.html#joining-on-multiple-tables
        query = (ProtoApp.select(Project, ProtoApp, App, ProtoAppDependency, Sample)
                 .join(Project)
                 .switch(ProtoApp)
                 .join(App)
                 .switch(ProtoApp)
                 .join(ProtoAppDependency)
                 .join(Sample, JOIN_LEFT_OUTER)
                 .group_by(ProtoApp)
        )
        # a fair amount of repetition here
        # but I decided I preferred this to a more convoluted generic mechanism
        if "project" in constraints:
            query_field = Project.name
            query = self._augment_query(query, query_field, constraints["project"], exact)
        if "sample" in constraints:
            query_field = Sample.name
            query = self._augment_query(query, query_field, constraints["sample"], exact)
        if "status" in constraints:
            query_field = ProtoApp.status
            query = self._augment_query(query, query_field, constraints["status"], exact)
        if "name" in constraints:
            query_field = App.name
            query = self._augment_query(query, query_field, constraints["name"], exact)

        return [x for x in query]

    ######
    # Possibly this should be part of the sample object in ORM...
    @staticmethod
    def get_sample_relationships(sample):
        try:
            sr = sample.samplerelationship
            # if there is not relationship, this will throw an exception
            fs = sr.fromsample
            return sr
        except:
            raise DataAccessLayer.DBMissingException("sample relationship could not be found")

    ######
    # Possibly this should be part of the sample object in ORM...
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

    def get_all_apps_by_substring(self, app_substr):
        like_str = self._make_like_string(app_substr)
        all_matches_query = App.select().where(App.name % like_str)
        return [x for x in all_matches_query]

    def get_one_app_by_substring(self, app_substr):
        all_matches = self.get_all_apps_by_substring(app_substr)
        if len(all_matches) == 0:
            raise DataAccessLayer.DBMissingException("Found no apps!")
        elif len(all_matches) > 1:
            all_app_names = ",".join([str(x.name) for x in all_matches])
            raise DataAccessLayer.DBFormatException(
                "Found too many apps matching %s (%s): be more specific!" % (app_substr, all_app_names))
        else:
            return all_matches[0]
