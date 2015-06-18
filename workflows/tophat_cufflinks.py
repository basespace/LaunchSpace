__author__ = 'psaffrey'

import os
import sys

# Add relative path libraries
SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.append(os.path.abspath(os.path.sep.join([SCRIPT_DIR, "..", "lib"])))

import WorkflowBuilder


def build(wb, control_list, comparison_list):
    control_pas = []
    for sample in control_list:
        control_pa = wb.TopHatAlignment([sample])
        control_pas.append(control_pa)

    comparison_pas = []
    for sample in comparison_list:
        comparison_pa = wb.TopHatAlignment([sample])
        comparison_pas.append(comparison_pa)

    cufflinks_pa = wb.CufflinksAssemblyDE(control_pas, comparison_pas)


if __name__ == "__main__":
    project_name = "Sloths Test"
    visualize_only = True

    if visualize_only:
        wv = WorkflowBuilder.workflow_visualiser_factory()
        control_list = WorkflowBuilder.SampleProviderAbstract(4)
        comparison_list = WorkflowBuilder.SampleProviderAbstract(4)
        build(wv, control_list, comparison_list)
        wv.create_dot("/home/psaffrey/tmp/workflow.dot")
    else:
        wb = WorkflowBuilder.workflow_builder_factory(project_name)
        control_list = WorkflowBuilder.SampleProviderList(
            ["RZ100ngHuBr_i6_E1_01", "RZ100ngHuBr_i6_F1_02", "RZ100ngHuBr_i6_G1_03", "RZ100ngHuBr_i6_H1_04"])
        comparison_list = WorkflowBuilder.SampleProviderList(
            ["RZ100ngUHR_i5_A1_01", "RZ100ngUHR_i5_B1_02", "RZ100ngUHR_i5_C1_03", "RZ100ngUHR_i5_D1_04"])
        build(wb, control_list, comparison_list)

