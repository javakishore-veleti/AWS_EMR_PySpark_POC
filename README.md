# AWS_EMR_PySpark_POC
A fusion of AWS EMR PySpark Proof of concept

## MongoDB Connection URI
You need to set an environment variable (preferably MongoDB ATLAS) connection URI

```shell

# Example 
# mongodb_uri = "mongodb+srv://<username>:<password>@<cluster_name>.<other_identifier>.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# If your cluster name is different from "Cluster0", change the same in the above URI

export MONGODB_ATLAS_CONNECTION_URL="<above mongodb_url pattern>"

# [app_main.py] reads the above environment variable, search for this code snipper in this repository
# mongodb_uri = os.getenv("MONGODB_ATLAS_CONNECTION_URL")

```

## Requirements To Run PySpark ETL
1. Mongodb Cluster (preferably sharded, and/or MongoDB ATLAS)
2. AWS EMR cluster setup
3. Docker on your desktop (optional) - required only if you need to run local MongoDB cluster and not above MongoDB cluster (i.e. point no 1)

## Running the PySpark ETL

```shell

# Optional (only if local mongodb server)
# npm run MongoDb_Cluster_Up 

npm run Etl_Stg2Raw_PySpark_MongoDb

# Optional (only if local mongodb server)
# npm run MongoDb_Cluster_Down 

```