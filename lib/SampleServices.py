"""
Services to access BaseSpace "sample" information using BaseSpace v1pre3 API
"""

import os, sys
import logging

# Add relative path libraries
SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, os.path.abspath(os.path.sep.join([SCRIPT_DIR, "..", "..", "basespace-python-sdk", "src"])))

from BaseSpacePy.api.BaseSpaceAPI import BaseSpaceAPI
from BaseSpacePy.model.QueryParameters import QueryParameters
from collections import defaultdict
from memoize import memoized
from operator import attrgetter
import Repository

NUMREADS_ATTR = "NumReadsPF"
READ1_ATTR = "Read1"
READ2_ATTR = "Read2"
PAIRED_END_ATTR = "IsPairedEnd"

baseSpaceAPI = BaseSpaceAPI()
noLimitQP = QueryParameters({ "Limit" : 1000 })

def OrganiseSamples(allSampleList):
    """
    GetSamplesInProject() returns a flat list of samples. 
    This rearranges it a little to make it easier to work with

    @param allSampleList: (list of BaseSpace sample objects)

    @return (dict): sample_name -> list of BaseSpace sample objects
    """
    sampleListsByName = defaultdict(list)
    for sample in allSampleList:
        # don't forget! a BaseSpace "Sample" is a bundle of fastq files
        # there could be more than one bundle with the same sample name
        # but these are distinct as far as BaseSpace is concerned
        # so we make a list of all the BaseSpace samples with the same samplename
        sampleListsByName[sample.SampleId].append(sample)
    # sort each list by date so we can easily get the most recent
    for sampleName, sampleList in sampleListsByName.iteritems():
        sampleList.sort(key=attrgetter("DateCreated"), reverse=True)
    return sampleListsByName


@memoized
def GetSamplesInProject(projectId):
    """
    get all the basespace sample objects for a given project projectId
    note the use of the "noLimitQP", a BaseSpaceAPI QueryParameters object that ensures we get as many as we can (up to 1000)

    @param projectId: (str) BaseSpace ID for project

    @return (dict): sample_name -> list of BaseSpace sample objects
    """
    logging.debug("retrieving samples from BaseSpace")
    sampleList = baseSpaceAPI.getSamplesByProject(projectId, noLimitQP)
    organised = OrganiseSamples(sampleList)
    logging.debug("Found: %s" % (organised.keys()))
    return organised

def GetMostRecentSampleFromSampleName(sampleName, projectId):
    """
    Get the most recent BaseSpace sampel object from the sample name.
    Relies on date ordering provided by OrganiseSamples()

    @param sampleName: (str)
    @param projectId: (str)
    """
    samples = GetSamplesInProject(projectId)
    # uses the sort-by-date to return the most recent!
    return samples[sampleName][0]

def GetSampleYield(sampleName, projectId):
    sample = GetMostRecentSampleFromSampleName(sampleName, projectId)
    readLength = getattr(sample, READ1_ATTR)
    numReads = getattr(sample, NUMREADS_ATTR)
    if getattr(sample, PAIRED_END_ATTR):
        assert getattr(sample, READ1_ATTR) == getattr(sample, READ1_ATTR), "cannot measure yield on sample with unequal read lengths!"
        # number of reads * 2 for paired end and * by readlength gives yield in bases
        sampleYield = float(numReads) * 2 * readLength
    else:
        # TODO: do we even want to support single-end runs like this?
        sampleYield = float(numReads) * readLength
    return sampleYield

def SampleHasData(sampleName, projectId):
    sampleListsByName = GetSamplesInProject(projectId)
    return sampleName in sampleListsByName

