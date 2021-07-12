"""@package piezo.consts
Global constants like directory paths and so on.
"""

__author__ = "Kevin Woldt"
__copyright__ = "Copyright (C) 2017"
__license__ = "MIT License"

import os.path

# global constant to probes discovery configuration files directory
DISCOVERY_CONFIG_DIR = os.path.join(
    '../..', 'runtime', 'probeManager', 'discoveryConfigFiles')
