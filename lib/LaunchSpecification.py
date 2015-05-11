import copy
import json

__author__ = 'psaffrey'


class LaunchSpecificationException(Exception):
    pass


class LaunchSpecification(object):
    LAUNCH_HEADER = {
        # the "Name" will be filled in when the launch json is created
        #  "Name": "{{ LaunchName }}",
        "StatusSummary": "AutoLaunch",
        "AutoStart": True,
    }

    def __init__(self, properties, defaults, configuration_services):
        self.properties = properties
        self.property_lookup = dict((self.clean_name(property_["Name"]), property_) for property_ in self.properties)
        self.defaults = defaults
        self.BS_ENTITIES = configuration_services.get_config("BS_ENTITIES")
        self.BS_ENTITY_LIST_NAMES = [self.bs_entity_to_bs_entity_list_name(ent) for ent in self.BS_ENTITIES]
        self.API_VERSION = configuration_services.get_config("ApiVersion")

    @staticmethod
    def bs_entity_to_bs_entity_list_name(bs_entity):
        return bs_entity.title() + "s"

    @staticmethod
    def clean_name(parameter_name):
        prefix, cleaned_name = parameter_name.split(".")
        assert prefix == "Input"
        return cleaned_name

    def listify_variables(self, var_dict):
        # if an input requirement is a list, the user will supply it comma separated
        # here we'll need to turn it into a list
        for varname in var_dict:
            varval = var_dict[varname]
            if varname not in self.property_lookup:
                continue
            vartype = self.property_lookup[varname]["Type"]
            if "[]" in vartype and not isinstance(varval, list):
                # turn it into a list
                varval = varval.split(",")
                var_dict[varname] = varval

    def populate_properties(self, var_dict):
        populated_properties = copy.copy(self.properties)
        bs_entity_lists = dict(((bs_entity, list()) for bs_entity in self.BS_ENTITIES))
        for property_ in populated_properties:
            property_name = self.clean_name(property_["Name"])
            property_type = property_["Type"]
            bald_type = str(property_type).translate(None, "[]")
            property_value = var_dict[property_name]
            # if the property is a BaseSpace entity, preprend a URI prefix
            if bald_type in self.BS_ENTITIES:
                if "[]" in property_type:
                    property_value = ["%s/%ss/%s" % (self.API_VERSION, bald_type, one_val) for one_val in
                                      property_value]
                else:
                    property_value = "%s/%ss/%s" % (self.API_VERSION, bald_type, property_value)
            if "[]" in property_type:
                property_["items"] = property_value
            else:
                property_["Content"] = property_value
            # gather up BaseSpace core entities:
            if bald_type in self.BS_ENTITIES:
                if "[]" in property_type:
                    bs_entity_lists[bald_type].extend(property_value)
                else:
                    bs_entity_lists[bald_type].append(property_value)
        return populated_properties

    def get_variable_requirements(self):
        return set((self.clean_name(property_["Name"]) for property_ in self.properties))

    def get_minimum_requirements(self):
        all_variables = self.get_variable_requirements()
        all_defaults = set(self.defaults.keys())
        return all_variables - all_defaults

    def get_property_type(self, varname):
        for property_ in self.properties:
            if self.clean_name(property_["Name"]) == varname:
                return str(property_["Type"])
        raise LaunchSpecificationException("asking for type for unknown variable: %s" % varname)

    def get_property_bald_type(self, varname):
        """
        same as get_property_type, but strip off any list specifier
        """
        return self.get_property_type(varname).translate(None, "[]")

    def is_list_property(self, varname):
        return "[]" in self.get_property_type(varname)

    def make_launch_json(self, user_supplied_vars, launch_name):
        supplied_var_names = set(user_supplied_vars.keys())
        required_vars = self.get_minimum_requirements()
        if required_vars - supplied_var_names:
            raise LaunchSpecificationException(
                "Compulsory variable(s) missing! (%s)" % required_vars - supplied_var_names)
        if supplied_var_names - self.get_variable_requirements():
            print "warning! unused variable(s) specified: (%s)" % str(
                supplied_var_names - self.get_variable_requirements())
        all_vars = copy.copy(self.defaults)
        all_vars.update(user_supplied_vars)
        self.listify_variables(all_vars)
        # build basic headers
        launch_dict = copy.copy(self.LAUNCH_HEADER)
        launch_dict["Name"] = launch_name
        properties_dict = self.populate_properties(all_vars)
        launch_dict["Properties"] = properties_dict
        return json.dumps(launch_dict)

    def dump_property_information(self):
        print "\t".join(["Name", "Type", "Default"])
        minimum_requirements = self.get_minimum_requirements()
        for property_ in self.properties:
            property_name = self.clean_name(property_["Name"])
            if property_name in minimum_requirements:
                continue
            property_type = property_["Type"]
            output = [property_name, property_type]
            if property_name in self.defaults:
                output.append(str(self.defaults[property_name]))
            print "\t".join(output)
