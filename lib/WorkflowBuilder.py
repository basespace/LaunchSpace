__author__ = 'psaffrey'

from LaunchSpecification import LaunchSpecification
from DataAccessRead import DataAccessRead
from DataAccessCreate import DataAccessCreate

class WorkflowBuilder(object):
    def __init__(self, data_access_read, data_access_create):
        assert isinstance(data_access_read, DataAccessRead)
        assert isinstance(data_access_create, DataAccessCreate)
        self._data_access_read = data_access_read
        self._data_access_create = data_access_create

    def create_proto_app_with_dependencies(self, app_name, arg_list):
        """
        Create a protoapp and link it to its dependencies
        does not currently support overwritng default configuration variables
        """
        app = self._data_access_read.get_app_by_name(app_name)
        launch_spec = LaunchSpecification(app.properties, app.defaults)
        parameters = launch_spec.get_minimum_requirements()
        parameter_types = [launch_spec.get_property_type(parameter) for parameter in parameters]
        assert len(parameters) == len(parameter_types) == len(arg_list)
        # create the protoapp
        self._data_access_create.add_sample_app("dummy!", app_name)
        # create the dependencies
        call_spec = zip(parameters, parameter_types, arg_list)
        for param_spec in call_spec:
            self.add_dependency(*param_spec)

    def add_dependency(self, parameter, type, argument):
        pass