__author__ = 'psaffrey'

import os
import sys

# Add relative path libraries
SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.append(os.path.abspath(os.path.sep.join([SCRIPT_DIR, "..", "lib"])))

import WorkflowBuilder


def build(wb, samples):

    for sample in samples:
        beetl_pa = wb.BEETLBWTBuilder([sample])
        pgapp_pa = wb.PGapp1VCF2kmers(beetl_pa.outputs["firstidx"])


if __name__ == "__main__":
    project_name = "Sloths Test"
    visualize_only = False

    if visualize_only:
        wv = WorkflowBuilder.workflow_visualiser_factory()
        build(wv, WorkflowBuilder.SampleProviderAbstract(10))
        wv.create_dot("/home/psaffrey/tmp/workflow.dot")
    else:
        wb = WorkflowBuilder.workflow_builder_factory(project_name)
        sample_list = ["B2_LIB36"]
        build(wb, WorkflowBuilder.SampleProviderList(sample_list))

