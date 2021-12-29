from feast import FeatureStore


class MwFeatureStore:
    __instance = None
    _mwfs = None

    @staticmethod
    def get_instance():
        if MwFeatureStore.__instance is None:
            MwFeatureStore()

        return MwFeatureStore._mwfs

    def __init__(self):
        if MwFeatureStore.__instance is not None:
            raise Exception("This class is a singleton")
        else:
            MwFeatureStore.__instance = self
            MwFeatureStore._mwfs = FeatureStore(repo_path="./")
