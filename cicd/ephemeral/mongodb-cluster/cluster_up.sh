PWD_VAL=$(pwd)
echo "PWD_VAL ${PWD_VAL}"
LOG_PREFIX="cluster_up.sh "

echo ""
echo "${LOG_PREFIX} Entered PWD_VAL=${PWD_VAL}"

# import common_vars.sh (observe dot at the beginning of the line)
. "${PWD_VAL}/cicd/ephemeral/mongodb-cluster/cluster_vars_common.sh"

docker network create ${MONGO_CLUSTER_NETWORK_NAME}  || true

node_counter=0

MONGODB_DATA_PATH_LOCAL="${PWD_VAL}/cicd/ephemeral/mongodb-cluster/mongo-data/db"
REPLICA_INITIALIZE_MEMBERS_IDS_TEXT=""
for node_name in "${MONGODB_NODE_NAMES[@]}"; do

  NODE_FULL_NAME="${NODE_NAME_PREFIX}${node_name}"
  echo "${LOG_PREFIX} Creating MongoDB Node NODE_FULL_NAME ${NODE_FULL_NAME}"

echo "${LOG_PREFIX} MONGO_CLUSTER_KEYFILE_PATH ${MONGO_CLUSTER_KEYFILE_PATH}"

  NODE_SPECIFIC_MONGODB_DATA_PATH="${MONGODB_DATA_PATH_LOCAL}/${node_name}/data/db"
  echo "${LOG_PREFIX} Deleting NODE_SPECIFIC_MONGODB_DATA_PATH ${NODE_SPECIFIC_MONGODB_DATA_PATH} IF EXISTED BEFORE"
  rm -rf ${NODE_SPECIFIC_MONGODB_DATA_PATH} || true
  echo "${LOG_PREFIX} Creating NODE_SPECIFIC_MONGODB_DATA_PATH ${NODE_SPECIFIC_MONGODB_DATA_PATH}"
  mkdir -p ${NODE_SPECIFIC_MONGODB_DATA_PATH}/${node_name} || true
  echo ""

  echo "${LOG_PREFIX} MONGO_CLUSTER_NETWORK_NAME ${MONGO_CLUSTER_NETWORK_NAME}"
  echo ""
  docker run --name ${NODE_FULL_NAME} --hostname ${NODE_FULL_NAME} -d \
  -p 192.168.1.131:${PORT_NO_START}:27017 \
  --net ${MONGO_CLUSTER_NETWORK_NAME} \
  -e MONGO_INITDB_ROOT_USERNAME=${USERNAME} \
  -e MONGO_INITDB_ROOT_PASSWORD=${PASSWORD} \
  --security-opt seccomp=unconfined \
  -v ${MONGO_CLUSTER_KEYFILE_PATH}:/etc/mongo-keyfile \
  -v ${NODE_SPECIFIC_MONGODB_DATA_PATH}:/data/db \
  $MONGO_DOCKER_IMAGE:7.0.9-jammy --replSet $REPLICA_SET_NAME \
  --keyFile /etc/mongo-keyfile --auth

  comma_suffix=""
  if [ "$node_counter" -gt 0 ] && [ "$node_counter" -lt "$MONGODB_NODES_COUNT" ]; then
    comma_prefix=", "
  fi

  PORT_NO_START_FOR_REPLICATION=${PORT_NO_START}
  PORT_NO_START_FOR_REPLICATION=27017
  if [ "$node_counter" -eq 0 ]; then
    PORT_NO_START_FOR_REPLICATION=27017
  fi
  node_specific_replica_member_id_text="${comma_prefix} { _id: ${node_counter}, host: \"${NODE_FULL_NAME}:${PORT_NO_START_FOR_REPLICATION}\" }"
  REPLICA_INITIALIZE_MEMBERS_IDS_TEXT="${REPLICA_INITIALIZE_MEMBERS_IDS_TEXT}${node_specific_replica_member_id_text} "

  PORT_NO_START=$((PORT_NO_START+1))
  node_counter=$((node_counter+1))

  echo ""

done

# Wait for MongoDB to start up (necessary before initiating the replica set)
echo "${LOG_PREFIX} Waiting for MongoDB nodes to start..."
sleep 10

<<'END_COMMENT'

# echo "${LOG_PREFIX} FIRST_NODE_NAME_WITH_PREFIX ${FIRST_NODE_NAME_WITH_PREFIX}"
# docker exec -it ${FIRST_NODE_NAME_WITH_PREFIX} mongo -u ${USERNAME} -p ${PASSWORD} --authenticationDatabase admin || true

# "rs.initiate({
#  _id: '$REPLICA_SET_NAME',
#  members: [
#    { _id: 0, host: 'mongo-node1:47017' },
#    { _id: 1, host: 'mongo-node2:47018' },
#    { _id: 2, host: 'mongo-node3:47019' }
#  ]
# })"


#docker exec aws-emr-pyspark-mongo-node1 mongosh -u admin -p password --authenticationDatabase admin --eval "rs.initiate({
#"  _id: 'rs0',
#"  members: [
#    { _id: 0, host: "aws-emr-pyspark-mongo-node1:47017"},
#    { _id: 1, host: "aws-emr-pyspark-mongo-node2:47018" },
#    { _id: 2, host: "aws-emr-pyspark-mongo-node3:47019" },
#  ]
#})"
END_COMMENT

echo "${LOG_PREFIX} REPLICA_INITIALIZE_MEMBERS_IDS_TEXT ${REPLICA_INITIALIZE_MEMBERS_IDS_TEXT}"
echo "${LOG_PREFIX} FIRST_NODE_NAME_WITH_PREFIX ${FIRST_NODE_NAME_WITH_PREFIX}"

RS_INITIALIZE_TEXT="rs.initiate({ _id: '$REPLICA_SET_NAME', members: [ $REPLICA_INITIALIZE_MEMBERS_IDS_TEXT ] } )"
docker exec ${FIRST_NODE_NAME_WITH_PREFIX} mongosh -u $USERNAME -p $PASSWORD --authenticationDatabase admin --eval "${RS_INITIALIZE_TEXT}"

echo "${LOG_PREFIX} Replica set initiated."

<<'END_COMMENT'
NODE_FULL_NAME="${NODE_NAME_PREFIX}mongo-express"
docker run -d \
  --name ${NODE_FULL_NAME} --hostname ${NODE_FULL_NAME} \
  --net ${MONGO_CLUSTER_NETWORK_NAME} \
  -e ME_CONFIG_MONGODB_SERVER=${FIRST_NODE_NAME_WITH_PREFIX} \
  -e ME_CONFIG_MONGODB_SERVER=aws-emr-pyspark-mongo-node1 \
  -e ME_CONFIG_MONGODB_PORT=27017 \
  -e ME_CONFIG_MONGODB_ADMINUSERNAME=$USERNAME \
  -e ME_CONFIG_MONGODB_ADMINPASSWORD=$PASSWORD \
  -e ME_CONFIG_MONGODB_AUTH_DATABASE=admin \
  -e ME_CONFIG_MONGODB_ENABLE_ADMIN=true \
  -e useUnifiedTopology=true \
  -e ME_CONFIG_MONGODB_URL="mongodb://$USERNAME:$PASSWORD@aws-emr-pyspark-mongo-node1:27017,aws-emr-pyspark-mongo-node2:27017,aws-emr-pyspark-mongo-node3:27017/?replicaSet=rs0&readPreference=secondaryPreferred" \
  -p 8081:8081 \
  mongo-express

# Open a web browser and go to http://localhost:8081 to access the Mongo Express web interface.

echo "${LOG_PREFIX} mongo-express = Open a web browser and go to http://localhost:8081 to access the Mongo Express web interface."
END_COMMENT

