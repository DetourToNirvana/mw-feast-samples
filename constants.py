from feast import ValueType

DATA_API_BASE_PATH = "http://fs-data-dev.mobilewalla.com:9898"
ADD_COLUMN_DATA_API = "/v2/sample/transform/addColumn/"
REQUEST_STATUS_DATA_API = "/v2/request/status/"
FEATURE_GROUP_DATA_API = "/v2/sample/feature-group/"
FS_BASE_S3_PATH = "s3://mwfs-betaclient1-roots3/feature-store"
FS_S3_BUCKET_NAME = "mwfs-betaclient1-roots3"

FEATURE_VIEW_NAME_PREFIX = "fv_"

FEATURE_STORE_REPO = "/Users/sid/dev/code/feast_repo/charmed_escargot/"

DataToValueType = {"integer": ValueType.INT32,
                   "long": ValueType.INT64,
                   "string": ValueType.STRING,
                   "double": ValueType.DOUBLE,
                   "float": ValueType.FLOAT,
                   "boolean": ValueType.BOOL,
                   "unix_timestamp": ValueType.UNIX_TIMESTAMP}

DataToDBType = {"integer": "INT",
                "long": "BIGINT",
                "string": "VARCHAR(max)",
                "double": "DOUBLE PRECISION",
                "float": "REAL",
                "boolean": "BOOLEAN",
                "unix_timestamp": "BIGINT"}

DataToAVROFieldsType = {"integer": "\"int\"",
                "long": "\"bigint\"",
                "string": "\"string\"",
                "double": "{\"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": 8, \"scale\": 2}",
                "float": "{\"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": 8, \"scale\": 2}",
                "boolean": "\"boolean\"",
                "unix_timestamp": "\"bigint\""}

# Flags for controlling environment-specific processing
USE_AWS_PROFILE_IN_BOTO3 = True  # Set to false if you already have the right profile to access the service (Eg: Assumed Role)
BOTO3_AWS_PROFILE_NAME = "mwfs-beta"
SQS_QUEUE_NAME = "feast-requests-sid.fifo"
MATERIALIZATION_SQS_QUEUE_NAME = "feast-materialization-sid.fifo"
AWS_REGION_NAME = "ap-southeast-2"
