from datetime import datetime
import logging
import threading

from services.MwFeatureStoreService import MwFeatureStore


class MaterializationService(threading.Thread):
    def __init__(self, threadID, name, fv_name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.fv_name = fv_name

    def run(self):
        try:
            logging.info("Starting " + self.name)

            # inc_mat till now.
            MwFeatureStore.get_instance().materialize_incremental(end_date=datetime.now(),
                                                                  feature_views=[self.fv_name])
        except Exception as e:
            logging.error("Error while materializing: Exception: {}".format(e))
