"""
Services to access locally stored *static* configuration information, as opposed to the dynamic information provided by users
designed to hide the details of the underlying config

"""

import os
import imp

# Add relative path libraries
SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))


class ConfigException(Exception):
    pass


class ConfigurationServices(object):
    def __init__(self, config_path=""):
        if not config_path:
            config_path = os.path.join(SCRIPT_DIR, "../etc", "config.py")
        self.config = imp.load_source("config", config_path)

    def get_config(self, key):
        try:
            return getattr(self.config, key)
        except AttributeError as err:
            raise ConfigException("Problem in getting config value: %s (%s)" % (key, str(err)))


