__author__ = 'psaffrey'

from collections import defaultdict

from LaunchSpecification import LaunchSpecification
from DataAccessRead import DataAccessRead
from DataAccessCreate import DataAccessCreate
from DataAccessLayer import DBMissingException
from ConfigurationServices import ConfigurationServices
# these are needed for type assertions
from DataAccessORM import ProtoApp, ProtoAppOutputDescription, AppConsumes

NODE_SHAPES = {
    "sample": "box",
    "protoapp": "circle"
}


class WorkflowBuilderException(Exception):
    pass


def workflow_builder_factory(project_name):
    configuration_services = ConfigurationServices()
    db_config = configuration_services.get_config("DBFile")
    data_access_read = DataAccessRead(db_config, configuration_services)
    data_access_create = DataAccessCreate(db_config, configuration_services)
    wb = WorkflowBuilder(project_name, data_access_read, data_access_create, configuration_services)
    wb.wrap_apps_as_methods()
    return wb


def workflow_visualiser_factory():
    configuration_services = ConfigurationServices()
    db_config = configuration_services.get_config("DBFile")
    data_access_read = DataAccessRead(db_config, configuration_services)
    wi = WorkflowVisualiser(data_access_read, configuration_services)
    wi.wrap_apps_as_methods()
    return wi


class SampleProvider(object):
    pass


class SampleProviderList(SampleProvider):
    def __init__(self, inlist):
        self._inlist = inlist

    def __iter__(self):
        return iter(self._inlist)


class SampleProviderFile(SampleProvider):
    def __init__(self, infile):
        self._infile = infile

    def __iter__(self):
        with open(self._infile) as fh:
            for line in fh:
                yield line.strip()


class SampleProviderAbstract(SampleProvider):
    BASE = "Sample"

    def __init__(self, num_samples):
        self._num_samples = num_samples

    def __iter__(self):
        for i in range(self._num_samples):
            yield "%s_%d" % (self.BASE, i + 1)


class WorkflowNode(object):
    def __init__(self, nodename, nodetype):
        self.nodename = nodename
        self.nodetype = nodetype
        # edge name -> WorkflowNode
        self.edges = {}

    def add_edge(self, tonode, label):
        self.edges[label] = tonode

    def write_node_dot(self, fh):
        node_shape = NODE_SHAPES[self.nodetype]
        print >> fh, "\"%s\" [ shape=%s ]" % (self.nodename, node_shape)

    def write_edges_dot(self, fh):
        for edge_label in self.edges:
            target_node = self.edges[edge_label]
            print >> fh, "\"%s\" -> \"%s\" [ label = \"%s\" ]" % (self.nodename, target_node.nodename, edge_label)

    # this is needed to duck type with a ProtoApp when constructing workflows
    def initialise_outputs(self):
        pass


# barebones graph implementation used by WorkflowVisualiser
class WorkflowGraph(object):
    def __init__(self):
        self.nodes = {}
        self.nodetypecount = defaultdict(int)

    def create_node(self, nodename, nodetype):
        if nodename in self.nodes:
            self.nodetypecount[nodename] += 1
            nodename = "%s (%d)" % (nodename, self.nodetypecount[nodename] + 1)
        node = WorkflowNode(nodename, nodetype)
        self.nodes[nodename] = node
        return node

    def create_edge(self, fromnode, tonode, label):
        self.nodes[fromnode.nodename].add_edge(tonode, label)

    def create_dot(self, filename):
        with open(filename, "w") as fh:
            print >> fh, "DiGraph G {"
            for node in self.nodes.values():
                node.write_node_dot(fh)
                node.write_edges_dot(fh)
            print >> fh
            print >> fh, "}"


# base class for objects that read workflow component dependencies
class WorkflowInterpreter(object):
    def __init__(self, data_access_read, configuration_services):
        self.app_list = []
        self._data_access_read = data_access_read
        self._configuration_services = configuration_services

    def wrap_apps_as_methods(self):
        self.app_list = []
        apps = self._data_access_read.get_all_apps()
        for app in apps:
            app_name = app.name
            flattened_name = app.get_flat_name()
            wrapped_method = self.wrap_proto_app_creation(app_name)
            setattr(WorkflowInterpreter, flattened_name, wrapped_method)
            self.app_list.append(flattened_name)

    def wrap_proto_app_creation(self, name):
        def wrapper(*args):
            return self.create_proto_app_with_dependencies(name, list(args[1:]))

        return wrapper

    def create_proto_app_with_dependencies(self, app_name, arg_list):
        """
        Create a protoapp and link it to its dependencies
        does not currently support overwritng default configuration variables
        """
        app = self._data_access_read.get_one_app_by_substring(app_name)
        launch_spec = LaunchSpecification(app.get_properties_as_dict(), app.get_defaults_as_dict(),
                                          self._configuration_services)
        parameters = launch_spec.get_minimum_requirements()
        # one of the parameters will always be the project!
        # create the protoapp
        pa = self.create_proto_app(app)
        # create the dependencies
        for parameter in parameters:
            parameter_type = launch_spec.get_property_type(parameter)
            if parameter_type == "project":
                # we don't need to record the project as a dependency; it's implicit
                continue
            else:
                arg = arg_list.pop(0)
                self.add_dependency(pa, parameter, parameter_type, arg)
        pa.initialise_outputs()
        return pa

    def create_proto_app(self, app):
        # pure virtual
        pass

    def add_dependency(self, pa, parameter, param_type, argument):
        # pure virtual
        pass

    @staticmethod
    def prepare_argument(param_type, argument):
        if "[]" in param_type:
            assert isinstance(argument, list), "non-list argument for list parameter: %s" % argument
        else:
            # wrap single arguments in list, so we can treat list and singular depdencies the same below
            argument = [argument]
        return argument


class WorkflowVisualiser(WorkflowInterpreter):
    def __init__(self, data_access_read, configuration_services):
        super(WorkflowVisualiser, self).__init__(data_access_read, configuration_services)
        self.workflow_graph = WorkflowGraph()

    def create_proto_app(self, app):
        return self.workflow_graph.create_node(app.name, "protoapp")

    def add_dependency(self, pa, parameter, param_type, argument):
        argument = self.prepare_argument(param_type, argument)
        if param_type.startswith("sample"):
            for sample_name in argument:
                sample_node = self.workflow_graph.create_node(sample_name, "sample")
                self.workflow_graph.create_edge(sample_node, pa, parameter)
        elif param_type.startswith("appresult"):
            for pa_dependency in argument:
                self.workflow_graph.create_edge(pa_dependency, pa, parameter)
        elif param_type.startswith("file"):
            for pa_dependency in argument:
                self.workflow_graph.create_edge(pa_dependency, pa, parameter)

    def create_dot(self, filename):
        self.workflow_graph.create_dot(filename)


class WorkflowBuilder(WorkflowInterpreter):
    def __init__(self, project_name, data_access_read, data_access_create, configuration_services):
        assert isinstance(data_access_read, DataAccessRead)
        assert isinstance(configuration_services, ConfigurationServices)
        super(WorkflowBuilder, self).__init__(data_access_read, configuration_services)
        assert isinstance(data_access_create, DataAccessCreate)
        self._data_access_create = data_access_create
        self.project = self._data_access_read.get_project_by_name(project_name)
        self.app_list = []

    def create_proto_app(self, app):
        return self._data_access_create.add_proto_app(app, self.project)

    def add_dependency(self, pa, parameter, param_type, argument):
        """
        The parameters here come from a zipping together of all the paramters to a proto_app from
        create_proto_app_with_dependencies

        pa: ProtoApp object
        parameter: the name of the dependency
        param_type: the BaseSpace parameter type (eg. string[])
        argument: the argument itself. The type of this depends on the param_type!
        """
        argument = self.prepare_argument(param_type, argument)
        # use the type to decide how to create the dependency
        # if it's a sample, lookup the sample and pass this as the id
        if param_type.startswith("sample"):
            for sample_name in argument:
                if not isinstance(sample_name, basestring):
                    raise WorkflowBuilderException("Sample must be a string")
                try:
                    sample = self._data_access_read.get_sample_by_name(sample_name)
                except DBMissingException:
                    sample = self._data_access_create.add_sample(sample_name, self.project.name)
                self._data_access_create.add_proto_app_dependency(pa, parameter, sample, None, None)
        # if it's an AppResult, pass the proto_app we depend on (which should be in the argument)
        elif param_type.startswith("appresult"):
            for pa_dependency in argument:
                if not isinstance(pa_dependency, ProtoApp):
                    raise WorkflowBuilderException("AppResult dependency needs to be a ProtoApp")
                self._data_access_create.add_proto_app_dependency(pa, parameter, None, pa_dependency, None)
        # if it's a file, pass the owning ProtoApp and the specific name of this output
        elif param_type.startswith("file"):
            for pa_dependency in argument:
                if not isinstance(pa_dependency, ProtoAppOutputDescription):
                    raise WorkflowBuilderException("File dependency needs to be ProtoAppOutput")
                upstream_pa = pa_dependency.owner
                input_name = pa_dependency.name
                # at this point we can check that the input_name provides a type that matches the input we're expecting
                # first, lookup the type of the supplied thing
                upstream_type = pa_dependency.type
                # then, lookup the description of the consumed thing and make sure it matches
                parameter_details = pa.app.get_input_by_name(parameter)
                assert isinstance(parameter_details, AppConsumes)
                if parameter_details.description != upstream_type:
                    msg = "Supplied type does not match consumed type! (wanted %s got %s)" % (
                        parameter_details.description, upstream_type)
                    raise WorkflowBuilderException(msg)
                self._data_access_create.add_proto_app_dependency(pa, parameter, None, upstream_pa, input_name)

