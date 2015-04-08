"""
Services to access locally stored *static* configuration information, as opposed to the dynamic information provided by users
designed to hide the details of the underlying config

"""

import os, sys

# Add relative path libraries
SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))

# currently store config as a Python file
sys.path.append(os.path.join(SCRIPT_DIR, "../etc"))
import config

class ConfigException(Exception):
    pass

def GetConfig(key):
    try:
        return getattr(config, key)
    except AttributeError as err:
        raise ConfigException("Problem in getting config value: %s (%s)" % (key, str(err)))


