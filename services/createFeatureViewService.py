import logging
import threading
from datetime import timedelta

import boto3
from feast import Entity, Feature, FeatureView, RedshiftSource

import constants
from constants import DataToValueType, FS_BASE_S3_PATH, DataToDBType, DataToAVROFieldsType
from services.MwFeatureStoreService import MwFeatureStore


class createFeatureViewThread(threading.Thread):
    def __init__(self, threadID, name, createFVRequest):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.createFVRequest = createFVRequest

    def run(self):
        try:
            logging.info("Starting " + self.name)

            if constants.USE_AWS_PROFILE_IN_BOTO3:
                session = boto3.session.Session(profile_name=constants.BOTO3_AWS_PROFILE_NAME,
                                                region_name=constants.AWS_REGION_NAME)
                redshift_client = session.client("redshift-data")
            else:
                redshift_client = boto3.client("redshift-data", region_name=constants.AWS_REGION_NAME)

            logging.info("self.createFVRequest: {}".format(str(self.createFVRequest)))

            _name = self.createFVRequest["name"]
            _primaryKeys = self.createFVRequest["primaryKeys"]
            _features = self.createFVRequest["features"]
            _online = self.createFVRequest["online"]
            _output = self.createFVRequest["output"]
            _featureGroupName = self.createFVRequest["featureGroupName"]
            _featureGroupVersion = self.createFVRequest["featureGroupVersion"]

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
                    name=pkName, join_key=pkName, value_type=DataToValueType[pkDatatype]
                )
                # registering entities to feature store
                MwFeatureStore.get_instance().apply([entity])
                entitiesList.append(pkName)
                logging.info(pkName)
                logging.info(DataToDBType[pkDatatype])
                query += " " + pkName + " " + DataToDBType[pkDatatype] + ","
                avroFieldsStr += "{\"name\":\"" + pkName + "\", \"type\":[ \"null\", " + DataToAVROFieldsType[pkDatatype] + " ],\"default\" : null},"

                # logging.info(entitiesList)

            featuresList = []
            for i in range(len(_features)):
                featureName = _features[i]["name"]
                featureDatatype = _features[i]["datatype"]
                featureObj = Feature(
                    name=featureName, dtype=DataToValueType[featureDatatype]
                )
                # logging.info(featureObj)
                featuresList.append(featureObj)
                logging.info(featureName)
                logging.info(DataToDBType[featureDatatype])
                query += " " + featureName + " " + DataToDBType[featureDatatype] + ","
                avroFieldsStr += "{\"name\":\"" + featureName + "\", \"type\":[ \"null\", " + DataToAVROFieldsType[featureDatatype] + " ],\"default\" : null},"

            query = query + " " + timeStampColumnsStr + ")"

            query = (
                    query
                    +
                    """
                        row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
                        location '"""+ str(FS_BASE_S3_PATH)+ """/"""+ str(_name)+ """/'
                        table properties ('wholeFile' = 'true');
                    """
            )

            logging.info(query)

            # feast.FeatureStore.config.json()
            response = redshift_client.execute_statement(
                ClusterIdentifier=MwFeatureStore.get_instance().config.offline_store.cluster_id,
                Database="feast-beta",
                DbUser="admin",
                Sql=query,
                StatementName="create-feature-view-external-table-" + _name,
                WithEvent=False,
            )

            logging.info("printing create table response")
            logging.info(response)

            # define feature view
            feature_view_t = FeatureView(
                name=_name,
                entities=entitiesList,
                features=featuresList,
                ttl=timedelta(weeks=52),
                online=_online,
                batch_source=RedshiftSource(
                    query="SELECT * FROM spectrum.fs_" + _name,
                    event_timestamp_column="event_timestamp",
                    created_timestamp_column="created_timestamp",
                ),
            )

            # Deploy the feature store to AWS
            logging.info("Deploying feature store to AWS: {}".format(str(feature_view_t)))
            MwFeatureStore.get_instance().apply([feature_view_t])

            logging.info("Exiting " + self.name)
        except Exception as e:
            logging.error(e)
