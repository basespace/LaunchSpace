__author__ = 'psaffrey'

import DataAccessLayer


class DataAccessDelete(DataAccessLayer.DataAccessLayer):

    def delete_samples(self, samples):
        with self.database.transaction():
            for sample in samples:
                sample.delete_instance()