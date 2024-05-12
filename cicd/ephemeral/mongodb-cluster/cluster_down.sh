#!/bin/bash

PWD_VAL=$(pwd)
LOG_PREFIX="cluster_down.sh "

echo ""
echo "${LOG_PREFIX} Entered PWD_VAL=${PWD_VAL}"

# import common_vars.sh (observe dot at the beginning of the line)
. "${PWD_VAL}/cicd/ephemeral/mongodb-cluster/cluster_vars_common.sh"


node_counter=0

REPLICA_INITIALIZE_MEMBERS_IDS_TEXT=""
for node_name in "${MONGODB_NODE_NAMES[@]}"; do
  NODE_FULL_NAME="${NODE_NAME_PREFIX}${node_name}"

  echo "${LOG_PREFIX} SHUTTING DOWN MongoDB Node NODE_FULL_NAME ${NODE_FULL_NAME}"
  echo "Stopping and removing MongoDB nodes...${NODE_FULL_NAME}"
  docker stop ${NODE_FULL_NAME} -|| true
  docker rm ${NODE_FULL_NAME} -|| true

  node_counter=$((node_counter+1))
done

NODE_FULL_NAME="${NODE_NAME_PREFIX}mongo-express"
echo "Removing the MongoDb Express Docker container ...${NODE_FULL_NAME}"
docker stop ${NODE_FULL_NAME} -|| true
docker rm ${NODE_FULL_NAME} -|| true

echo "Removing the Docker network...${MONGO_CLUSTER_NETWORK_NAME}"
docker network rm ${MONGO_CLUSTER_NETWORK_NAME} || true

echo "${LOG_PREFIX} MongoDB cluster and network have been shutdown and cleaned up."