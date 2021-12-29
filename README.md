## Setup
Install required dependencies by running "pip3 install -r requirements.txt" in the root of the project. You may prefer creating a virtual-env.

## Running
Add the following environment variables to the Run Configuration. You will need equivalent Aws resources to be 
substituted in the environment-variables below. Essentially, below provides all the values  

```
mwfs_config__project=charmed_escargot;
mwfs_config__registry=s3://mwfs-betaclient1-roots3/feast/registry/registry;
mwfs_config__provider=aws;mwfs_config__online_store__type=dynamodb;
mwfs_config__online_store__region=ap-southeast-2;mwfs_config__offline_store__type=redshift;
mwfs_config__offline_store__cluster_id=feastbetaredshiftcluster-etzn75aqranj;
mwfs_config__offline_store__region=ap-southeast-2;mwfs_config__offline_store__database=feast-beta;
mwfs_config__offline_store__user=admin;
mwfs_config__offline_store__s3_staging_location=s3://mwfs-betaclient1-roots3/feast/staging;
mwfs_config__offline_store__iam_role=arn:aws:iam::128669529570:role/feast-beta-instrole
```


## Original Run Logs
I have added the logs of my original run in log/test_registry_consistency.log