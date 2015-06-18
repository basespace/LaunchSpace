__author__ = 'psaffrey'

"""
Set of classes to help determine whether a ProtoApp's result is "ready" to be consumed by a downstream ProtoApp

The simplest case for readiness is just to check that the app is finished; this is defined in AppFinishedReadyChecker

Each method returns a DependencyReadinessResult() object, which wraps whether the dependency is ready and any details

Ideally, we would add checks to see if the upstream app has the file that we're after. We probably need to add
methods to app_services to support this
"""

# maps the name of an app to a class that checks whether
APP_CLASS_MAPPING = {}


def app_result_ready_checker_factory(app_name, app_services):
    if app_services.app_has_qc_conditions(app_name):
        return AppQCPassedReadyChecker(app_services)
    else:
        return AppFinishedReadyChecker(app_services)


def merge_dependency_readiness_results(drr_list):
    is_ready = all(drr_list)
    all_details = "::".join((drr.details for drr in drr_list if drr.details))
    return DependencyReadinessResult(is_ready, all_details)


class DependencyReadinessResult(object):
    def __init__(self, is_ready, details=""):
        self.is_ready = is_ready
        self.details = details

    def __nonzero__(self):
        return self.is_ready


class AppResultReadyChecker(object):
    def __init__(self, app_services):
        self._app_services = app_services

    def is_app_finished(self, proto_app):
        if not proto_app.basespaceid:
            return DependencyReadinessResult(False, "app not started!")
        is_finished = self._app_services.is_app_finished(proto_app.basespaceid)
        if is_finished:
            return DependencyReadinessResult(True)
        else:
            return DependencyReadinessResult(False, "app not finished!")

    def is_app_ready(self, proto_app):
        # pure virtual
        pass


class AppFinishedReadyChecker(AppResultReadyChecker):
    def is_app_ready(self, proto_app):
        return self.is_app_finished(proto_app)


class AppQCPassedReadyChecker(AppResultReadyChecker):
    def is_app_ready(self, proto_app):
        is_finished, details = self.is_app_finished(proto_app)
        if not is_finished:
            return is_finished, details
        failures = self._app_services.apply_automated_qc_to_app_result(proto_app)
        if failures:
            return DependencyReadinessResult(False, ";".join(failures))
        else:
            return DependencyReadinessResult(True)
