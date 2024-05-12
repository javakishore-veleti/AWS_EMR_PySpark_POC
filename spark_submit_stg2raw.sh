PWD_VAL=$(pwd)

LOG_PREFIX="spark_submit_stg2raw.sh "

echo ""
echo "${LOG_PREFIX} Entered PWD_VAL=${PWD_VAL}"

spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 --master local[3] ${PWD_VAL}/app_main.py --which_etl="Stg2Raw"

echo "${LOG_PREFIX} Exiting"