# create a key file and set appropriate permissions
# openssl rand -base64 756 > mongodb-keyfile
# chmod 400 mongodb-keyfile
MONGO_CLUSTER_NETWORK_NAME="aws-emr-pyspark-mongo-cluster-network"
MONGO_CLUSTER_KEYFILE_NAME="mongodb-keyfile"
MONGO_CLUSTER_KEYFILE_PATH="${PWD_VAL}/cicd/ephemeral/mongodb-cluster/${MONGO_CLUSTER_KEYFILE_NAME}"
MONGO_DOCKER_IMAGE="mongo"

NODE_NAME_PREFIX="aws-emr-pyspark-mongo-"

MONGODB_NODE_NAMES=("node1" "node2" "node3" )
MONGODB_NODES_COUNT=${#MONGODB_NODE_NAMES[@]}

FIRST_NODE_NAME="node1"
FIRST_NODE_NAME_WITH_PREFIX="${NODE_NAME_PREFIX}${FIRST_NODE_NAME}"
REPLICA_SET_NAME="rs0"
USERNAME="admin"
PASSWORD="password"

PORT_NO_START=47017

