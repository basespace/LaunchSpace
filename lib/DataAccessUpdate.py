__author__ = 'psaffrey'

import DataAccessLayer


class DataAccessUpdate(DataAccessLayer.DataAccessLayer):
    def set_sample_app_status(self, sample_app, newstatus, details=""):
        assert newstatus in self.PERMITTED_STATUSES, "bad status: %s" % newstatus
        if sample_app.status != newstatus or sample_app.statusdetails != details:
            sample_app.status = newstatus
            sample_app.statusdetails = details
            sample_app.save()

    @staticmethod
    def set_name_sample_app_session_id(self, sample_app, app_session_id):
        sample_app.basespaceid = app_session_id
        sample_app.save()

