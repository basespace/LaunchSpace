"""
 These classes allow convenient extraction of metadata from an app session to create app specifications for LaunchSpace
"""


def bs_entity_to_bs_entity_list_name(bs_entity):
    return bs_entity.lower() + "s"


SKIP_PROPERTIES = ["app-session-name"]
# if these strings are in the property names, we should not try to capture default values for them.
BS_ENTITIES = ["sample", "project", "appresult", "file"]
BS_ENTITY_LIST_NAMES = [bs_entity_to_bs_entity_list_name(ent) for ent in BS_ENTITIES]


class AppSessionMetaData(object):
    def __init__(self, appsession_metadata):
        self.asm = appsession_metadata

    def get_properties(self):
        # pure virtual
        return []

    @staticmethod
    def unpack_bs_property(bs_property, attribute):
        # pure virtual
        return ""

    def get_refined_appsession_properties(self):
        appsession_properties = self.get_properties()
        properties = []
        defaults = {}
        for as_property in appsession_properties:
            property_name = str(self.unpack_bs_property(as_property, "Name"))
            property_type = str(self.unpack_bs_property(as_property, "Type"))
            if not property_name.startswith("Input"):
                continue
            if property_name.count(".") != 1:
                continue
            property_basename = property_name.split(".")[-1]
            if property_basename in SKIP_PROPERTIES:
                continue
            if property_basename.lower() in BS_ENTITY_LIST_NAMES:
                continue
            this_property = {
                "Name": property_name,
                "Type": property_type,
            }
            properties.append(this_property)
            bald_type = property_type.translate(None, "[]")
            if bald_type in BS_ENTITIES:
                continue
            if property_type.endswith("[]"):
                default_var = self.unpack_bs_property(as_property, "Items")
                # this slightly odd logic is because of a BaseSpace bug. From Kent Ho:
                #
                #2.       For string array types of properties (string[]), for some reason, when you post with an array of length x,
                #the AppSession.json created has the same array but of length x+1 (with the first array element appended at the end an extra time).
                #For example, if you post ["1"], it becomes ["1","1"] or if you post ["1","2"], it becomes ["1","2","1"].
                #This could really mess up mapping or what not if people rely on the length of the array to assign settings
                #to samples or what not within the native app, and it's weird behavior that's unexpected.
                if len(default_var) == 1:
                    defaults[property_basename] = default_var
                else:
                    defaults[property_basename] = default_var[:-1]
            else:
                default_var = self.unpack_bs_property(as_property, "Content")
                defaults[property_basename] = default_var
        properties = self._trim_properties_app_results(properties)
        return properties, defaults

    @staticmethod
    def _trim_properties_app_results(properties):
        """
        special trimming to deal with file properties
        if there is a file property, we don't need any appresult properties, since they are taken care of by the file entries
        """
        has_file = any([prop["Type"] == "file" for prop in properties])
        if has_file:
            new_properties = [prop for prop in properties if prop["Type"].translate(None, "[]") != "appresult"]
        else:
            new_properties = properties
        return new_properties


class AppSessionMetaDataSDK(AppSessionMetaData):
    def get_properties(self):
        return self.asm.Properties.Items

    def get_app_name(self):
        return self.asm.Application.Name

    def get_app_id(self):
        return self.asm.Application.Id

    @staticmethod
    def unpack_bs_property(bs_property, attribute):
        return getattr(bs_property, attribute)


class AppSessionMetaDataRaw(AppSessionMetaData):
    def get_properties(self):
        return self.asm["Response"]["Properties"]["Items"]

    def get_app_name(self):
        return self.asm["Response"]["Application"]["Name"]

    def get_app_id(self):
        return self.asm["Response"]["Application"]["Id"]

    @staticmethod
    def unpack_bs_property(bs_property, attribute):
        return bs_property[attribute]

