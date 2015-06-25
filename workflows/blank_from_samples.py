__author__ = 'psaffrey'

import os
import sys

# to get this line to work, include the LaunchSpace lib directory in your $PYTHONPATH
import WorkflowBuilder


def build(wb, samples):

    for sample in samples:
        ########
        # Add your workflow here
        # this will be done once per supplied sample
        app_output = wb.myapp(sample) # CHANGE ME!
        wb.downstreamapp(app_output) # CHANGE ME!


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='My workflow based on some samples')
    parser.add_argument('-s', '--sample', type=str, dest="sample", help='specify just one sample')
    parser.add_argument('-f', '--file', type=str, dest="file", help='specify a file of samples')
    parser.add_argument('-v', '--visualize', type=str, dest="visualize_only", action="store_true", 
                                        help='do not create workflow, just create a visualization')

    if args.sample:
        assert not hasattr(args, "file"), "cannot specify file and sample together"
        sample_provider = WorkflowBuilder.SampleProviderList([args.sample])
    elif args.file:
        assert not hasattr(args, "sample"), "cannot specify file and sample together"
        sample_provider = WorkflowBuilder.SampleProviderFile([args.file])
    else:
        assert args.visualize, "either provide some samples or ask for a visualization!"
        sample_count = 10
        sample_provider = WorkflowBuilder.SampleProviderAbstract(sample_count)

    project_name = "My BaseSpace Project"
    visualize_only = args.visualize

    if visualize_only:
        # this will simulate what your workflow would look like with sample_count samples
        # and output this as a GraphViz .dot file
        # visualize it with "dot $HOME/workflow.dot -Tpng -o $HOME/workflow.png"
        wv = WorkflowBuilder.workflow_visualiser_factory()
        build(wv, sample_provider)
        wv.create_dot(os.getenv("HOME"), "workflow.dot")
    else:
        wb = WorkflowBuilder.workflow_builder_factory(project_name)
        build(wb, sample_provider)

