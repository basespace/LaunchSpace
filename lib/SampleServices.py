"""
Services to access BaseSpace "sample" information using BaseSpace v1pre3 API
"""

import os, sys
import logging

# Add relative path libraries
SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, os.path.abspath(os.path.sep.join([SCRIPT_DIR, "..", "..", "basespace-python-sdk", "src"])))

from BaseSpacePy.model.QueryParameters import QueryParameters
from collections import defaultdict
from memoize import memoized
from operator import attrgetter


class SampleServices(object):
    NUMREADS_ATTR = "NumReadsPF"
    READ1_ATTR = "Read1"
    READ2_ATTR = "Read2"
    PAIRED_END_ATTR = "IsPairedEnd"

    def __init__(self, basespace_api, configuration_services):
        self._basespace_api = basespace_api
        self._no_limit_qp = QueryParameters({"Limit": 1000})
        self._configuration_services = configuration_services

    @staticmethod
    def organise_samples(all_sample_list):
        """
        GetSamplesInProject() returns a flat list of samples.
        This rearranges it a little to make it easier to work with

        @param all_sample_list: (list of BaseSpace sample objects)

        @return (dict): sample_name -> list of BaseSpace sample objects
        """
        sample_lists_by_name = defaultdict(list)
        for sample in all_sample_list:
            # don't forget! a BaseSpace "Sample" is a bundle of fastq files
            # there could be more than one bundle with the same sample name
            # but these are distinct as far as BaseSpace is concerned
            # so we make a list of all the BaseSpace samples with the same samplename
            sample_lists_by_name[sample.SampleId].append(sample)
        # sort each list by date so we can easily get the most recent
        for sampleName, sampleList in sample_lists_by_name.iteritems():
            sampleList.sort(key=attrgetter("DateCreated"), reverse=True)
        return sample_lists_by_name

    @memoized
    def get_samples_in_project(self, project_id):
        """
        get all the basespace sample objects for a given project projectId
        note the use of the "noLimitQP", a BaseSpaceAPI QueryParameters object that ensures we get as many as we can (up to 1000)

        @param project_id: (str) BaseSpace ID for project

        @return (dict): sample_name -> list of BaseSpace sample objects
        """
        logging.debug("retrieving samples from BaseSpace")
        sample_list = self._basespace_api.getSamplesByProject(project_id, self._no_limit_qp)
        organised = self.organise_samples(sample_list)
        logging.debug("Found: %s" % (organised.keys()))
        return organised

    def get_most_recent_sample_from_sample_name(self, sample_name, project_id):
        """
        Get the most recent BaseSpace sample object from the sample name.
        Relies on date ordering provided by OrganiseSamples()

        @param sample_name: (str)
        @param project_id: (str)
        """
        samples = self.get_samples_in_project(project_id)
        # uses the sort-by-date to return the most recent!
        if sample_name in samples:
            return samples[sample_name][0]
        else:
            return None

    def get_sample_yield(self, sample_name, project_id):
        sample = self.get_most_recent_sample_from_sample_name(sample_name, project_id)
        read_length = getattr(sample, self.READ1_ATTR)
        num_reads = getattr(sample, self.NUMREADS_ATTR)
        if getattr(sample, self.PAIRED_END_ATTR):
            unequal_reads_error = "cannot measure yield on sample with unequal read lengths!"
            assert getattr(sample, self.READ1_ATTR) == getattr(sample, self.READ2_ATTR), unequal_reads_error
            # number of reads * 2 for paired end and * by readlength gives yield in bases
            sample_yield = float(num_reads) * 2 * read_length
        else:
            # single end
            sample_yield = float(num_reads) * read_length
        return sample_yield

    def sample_has_data(self, sample_name, project_id):
        sample_lists_by_name = self.get_samples_in_project(project_id)
        return sample_name in sample_lists_by_name

    def get_basespace_sample_id(self, sample_name, project_id):
        basespace_sample = self.get_most_recent_sample_from_sample_name(sample_name, project_id)
        if basespace_sample:
            return basespace_sample.Id
        else:
            return None

    def check_conditions_on_sample(self, sample_name, project_id, ignore_yield):
        """
        check if the sample has any fastqs and if they are of enough yield

        @param sampleName: (str)
        @param projectId: (str) basespace project ID
        @param ignoreYield: (bool)

        @return (bool): whether the conditions are met, (str): any details about why conditions are not met
        """
        yield_threshold = self._configuration_services.get_config("MinimumYield")
        if not self.sample_has_data(sample_name, project_id):
            return False, "No data"
        sample_yield = self.get_sample_yield(sample_name, project_id)
        if sample_yield > yield_threshold:
            return True, None
        else:
            if ignore_yield:
                return True, "Ignoring low yield!"
            return False, "Not enough yield (%d < %d)" % (sample_yield, yield_threshold)

