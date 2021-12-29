import logging
import os
import time
from datetime import timedelta

import boto3
from feast import FeatureView, Feature, Entity, RedshiftSource

import constants
from services.MwFeatureStoreService import MwFeatureStore
from services.createFeatureViewService import createFeatureViewThread
from services.materializationService import MaterializationService


def test_registry_consistency():
    _fv1_materializing = "trc_1p"
    _fv2_creating = "trc_2p"

    _project_base_path = "/Users/sid/IdeaProjects/learn/mw-feast-samples/"
    _test_file_path = _project_base_path + "sample-data/part-00000-a5375fc6-0abf-4b30-a3bc-e8541da51ef5-c000.csv"

    _createFV = {
        "name": _fv1_materializing,
        "primaryKeys": [
            {
                "name": "bundle",
                "datatype": "string"
            }
        ],
        "features": [
            {
                "name": "platform_category",
                "datatype": "string"
            },
            {
                "name": "asn",
                "datatype": "string"
            }
        ],
        "online": False,
        "output": {
            "fileType": "csv",
            "hasHeader": True
        },
        "featureGroupName": "xyz",
        "featureGroupVersion": "1.1.0"
    }

    # Create new thread
    _createFeatureViewT = createFeatureViewThread(1, "Thread-Create-Feature-View", _createFV)
    _createFeatureViewT.start()

    while _createFeatureViewT.is_alive():
        pass
    logging.info("creation of 1st FV completed")

    # Add data to feature view by copying the file to the expected S3 location.
    logging.info("copying data to required s3 location before starting materialization...")
    s3 = boto3.resource('s3')
    _s3_rel_path = "feature-store/" + _fv1_materializing + "/1/xyz.csv"
    s3.Bucket(constants.FS_S3_BUCKET_NAME).upload_file(_test_file_path, _s3_rel_path)
    logging.info("data copy to S3 done.")

    # Let's change the 1st FV to be an online store using feast apply.
    logging.info("Let's change the 1st FV to be an online store using feast apply.")

    _entityFeatureList = getFeaturesEntitiesFromCreateReq(_createFV)

    feature_view_t = FeatureView(
        name=_fv1_materializing,
        entities=_entityFeatureList["entitiesList"],
        features=_entityFeatureList["featuresList"],
        ttl=timedelta(weeks=52),
        online=True,
        batch_source=RedshiftSource(
            query="SELECT * FROM spectrum.fs_" + _fv1_materializing,
            event_timestamp_column="event_timestamp",
            created_timestamp_column="created_timestamp",
        ),
    )
    MwFeatureStore.get_instance().apply([feature_view_t])

    # Start materialization
    _materializeFeatureViewT = MaterializationService(2, "Thread-Materialize-FV", _fv1_materializing)
    _materializeFeatureViewT.start()

    # Let's give the materialization thread a head-start before creating a new FV:
    logging.info("Let's give the materialization thread a 20-sec head-start before creating a new FV...")
    time.sleep(20)

    pid = os.fork()
    if pid:
        # Parent process
        # Let's give the new FV creation process 15 seconds to complete.
        logging.info("waiting 20 seconds for the FV creation process to complete")
        time.sleep(20)
        try:
            logging.info(MwFeatureStore.get_instance().get_feature_view(_fv2_creating))
        except Exception as e:
            logging.info(
                "The process that is materializing the 1st FV is unaware that a new FV has been created! {}".format(e))

        # Wait for materialization to complete and then check if new FV appears in registry
        while _materializeFeatureViewT.is_alive():
            time.sleep(1)

        logging.info("materialization complete. Let's check if 2nd fv exists in registry now...")
        try:
            logging.info(MwFeatureStore.get_instance().get_feature_view(_fv2_creating))
        except Exception as e:
            logging.info(
                "2nd Feature View: {} does not exist in registry! Exception: {}".format(_fv2_creating, e))

    else:
        # Child process
        # Now create a new FV
        _createFV["name"] = _fv2_creating
        _createFeatureViewT_2 = createFeatureViewThread(3, "Thread-Create-Feature-View", _createFV)
        _createFeatureViewT_2.start()

        while _createFeatureViewT_2.is_alive():
            pass

        logging.info("creation of 2nd fv has completed")


def getFeaturesEntitiesFromCreateReq(create_fv_req):
    _name = create_fv_req["name"]
    _primaryKeys = create_fv_req["primaryKeys"]
    _features = create_fv_req["features"]
    _online = create_fv_req["online"]
    _output = create_fv_req["output"]
    _featureGroupName = create_fv_req["featureGroupName"]
    _featureGroupVersion = create_fv_req["featureGroupVersion"]

    _response = {
        "entitiesList": None,
        "featuresList": None
    }

    query = "create external table spectrum.fs_" + _name + "("
    avroFieldsStr = ""
    timeStampColumnsStr = "event_timestamp TIMESTAMP, created_timestamp TIMESTAMP"
    if _output["fileType"] == "parquet":
        timeStampColumnsStr = "event_timestamp VARCHAR(35), created_timestamp VARCHAR(35)"
    elif _output["fileType"] == "avro":
        timeStampColumnsStr = "event_timestamp TIMESTAMP, created_timestamp TIMESTAMP"

    entitiesList = []
    for i in range(len(_primaryKeys)):
        pkName = _primaryKeys[i]["name"]
        pkDatatype = _primaryKeys[i]["datatype"]
        entity = Entity(
            name=pkName, join_key=pkName, value_type=constants.DataToValueType[pkDatatype]
        )
        # registering entities to feature store
        MwFeatureStore.get_instance().apply([entity])
        entitiesList.append(pkName)
        logging.info(pkName)
        logging.info(constants.DataToDBType[pkDatatype])
        query += " " + pkName + " " + constants.DataToDBType[pkDatatype] + ","
        avroFieldsStr += "{\"name\":\"" + pkName + "\", \"type\":[ \"null\", " + constants.DataToAVROFieldsType[
            pkDatatype] + " ],\"default\" : null},"

    _response["entitiesList"] = entitiesList

    featuresList = []
    for i in range(len(_features)):
        featureName = _features[i]["name"]
        featureDatatype = _features[i]["datatype"]
        featureObj = Feature(
            name=featureName, dtype=constants.DataToValueType[featureDatatype]
        )
        # logging.info(featureObj)
        featuresList.append(featureObj)
        logging.info(featureName)
        logging.info(constants.DataToDBType[featureDatatype])
        query += " " + featureName + " " + constants.DataToDBType[featureDatatype] + ","
        avroFieldsStr += "{\"name\":\"" + featureName + "\", \"type\":[ \"null\", " + constants.DataToAVROFieldsType[
            featureDatatype] + " ],\"default\" : null},"

    _response["featuresList"] = featuresList

    return _response


# Run registry consistency test
test_registry_consistency()
