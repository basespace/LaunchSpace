import os
import sys

from urlparse import urljoin

# Add relative path libraries
SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))

PERMITTED_STATUSES = set(
    ["waiting", "submitted", "pending", "running", "launch-failed", "run-failed", "app-finished", "qc-failed",
     "qc-passed", "downloading", "download-failed", "downloaded"])

DEFAULT_STATUS = "waiting"

# map BaseSpace status to our own internal status
# I tried to use the BS app statuses from the Python SDK:
# statusAllowed = ['running', 'complete', 'needsattention', 'timedout', 'aborted']
# but these seem to be different to the actual BS statuses. These below are reverse engineered
STATUS_MAPPING = {
    "Complete": "app-finished",
    "Running": "running",
    "PendingExecution": "pending",
    "Aborted": "run-failed",
    "Initializing": "pending"
}

# naming
TN_RELATIONSHIP_NAME = "TumourNormal"
SAMPLE_LOG_DIR_NAME = "log"
QC_NAMESPACE = "AutomatedQC"

# basespace details
BaseSpaceHost = "http://api.cloud-hoth.illumina.com/"
ApiVersion = "v1pre3"
BaseSpaceBaseUri = urljoin(BaseSpaceHost, ApiVersion)
BS_ENTITIES = ["sample", "project", "file"]

# 105 Gigabases for a 30X genome
MinimumYield = 105000000000
#MinimumYield = 0

DBFile = os.path.join(SCRIPT_DIR, "../data/db.sqlite")

# logging
LogFormat = "%(asctime)s|%(levelname)s|%(message)s"
LOG_BASE = os.path.join(SCRIPT_DIR, "..", "log")
LAUNCHER_LOG_FILE = os.path.join(LOG_BASE, "launcher.log")
TRACKER_LOG_FILE = os.path.join(LOG_BASE, "tracker.log")
QCCHECKER_LOG_FILE = os.path.join(LOG_BASE, "qcchecker.log")
DOWNLOADER_LOG_FILE = os.path.join(LOG_BASE, "downloader.log")


# constant values
MAX_DOWNLOADS = 5
MAX_ATTEMPTS = 5

# execution details
PYTHON_EXE = sys.executable
