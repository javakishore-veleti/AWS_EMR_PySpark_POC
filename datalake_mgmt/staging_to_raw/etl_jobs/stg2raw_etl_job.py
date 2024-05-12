from pyspark.sql.types import StructType, StringType, StructField, IntegerType

from datalake_mgmt.common.configs.app_configs import AppConfigs
from datalake_mgmt.common.configs.config_keys import MONGODB_URI
from datalake_mgmt.common.constants.etl_exec_results import EtlExecResults
from datalake_mgmt.common.etl_framework.dtos import EtlExecCtx, EtlExecTaskCtx
from datalake_mgmt.util.pyspark_initializer import PySparkInitializer
import logging
import datetime

from datalake_mgmt.util.times_monitors import log_time

LOGGER = logging.getLogger(__file__)

stg2raw_schema = StructType([
    StructField("SeqNo", IntegerType(), True),
    StructField("CustomerId", IntegerType(), True),
    StructField("FromCurrency", StringType(), True),
    StructField("ToCurrency", StringType(), True),
    StructField("FromCurrencyAmount", IntegerType(), True),
    StructField("ToCurrencyAmount", IntegerType(), True),
    StructField("Trading_Date", StringType(), True),
    StructField("Trading_Date_YYYY", IntegerType(), True),
    StructField("Trading_Date_MM", IntegerType(), True),
    StructField("Trading_Date_DD", IntegerType(), True),
])

stg2raw_schema_read = StructType([
    StructField("_id", StringType(), True),
    StructField("SeqNo", IntegerType(), True),
    StructField("CustomerId", IntegerType(), True),
    StructField("FromCurrency", StringType(), True),
    StructField("ToCurrency", StringType(), True),
    StructField("FromCurrencyAmount", IntegerType(), True),
    StructField("ToCurrencyAmount", IntegerType(), True),
    StructField("Trading_Date", StringType(), True),
    StructField("Trading_Date_YYYY", IntegerType(), True),
    StructField("Trading_Date_MM", IntegerType(), True),
    StructField("Trading_Date_DD", IntegerType(), True),
])


# noinspection PyMethodMayBeStatic
class Stg2RawEtlJob:

    @log_time
    def op_create_pyspark_session(self, wf_ctx: EtlExecCtx, wf_task_ctx: EtlExecTaskCtx) -> int:
        LOGGER.info("ENTERED op_create_pyspark_session")

        # wf_ctx: EtlExecCtx, wf_task_ctx: EtlExecTaskCtx
        PySparkInitializer.create_pyspark_session(wf_ctx=wf_ctx, wf_task_ctx=wf_task_ctx)

        LOGGER.info("COMPLETED op_create_pyspark_session")
        return EtlExecResults.SUCCESS

    @log_time
    def read_stg_data(self, wf_ctx: EtlExecCtx, wf_task_ctx: EtlExecTaskCtx) -> int:
        LOGGER.info(f"ENTERED read_stg_data_store_in_mongodb stg2raw_dataset_location "
                    f"[{wf_task_ctx.stg2raw_dataset_location}]")

        input_df = wf_ctx.etl_exec_engine_ctx.spark_session.read.csv(wf_task_ctx.stg2raw_dataset_location, header=False,
                                                                     inferSchema=False, schema=stg2raw_schema)
        wf_task_ctx.etl_exec_engine_task_ctx.latest_df = input_df

        LOGGER.info("COMPLETED read_stg_data_store_in_mongodb")
        return EtlExecResults.SUCCESS

    @log_time
    def save_stg_data_in_mongodb(self, wf_ctx: EtlExecCtx, wf_task_ctx: EtlExecTaskCtx) -> int:
        LOGGER.info(f"ENTERED save_stg_data_in_mongodb stg2raw_dataset_location "
                    f"[{wf_task_ctx.stg2raw_dataset_location}]")

        input_df = wf_task_ctx.etl_exec_engine_task_ctx.latest_df

        mongodb_write_uri = AppConfigs.get_config(MONGODB_URI)

        num_partitions = 3  # Number of partitions you want
        partition_cols = ["CustomerId", "Trading_Date", "FromCurrency", "ToCurrency"]  # Partition columns

        # Repartition DataFrame by both count and columns
        partitioned_df = input_df.repartition(num_partitions, *partition_cols)

        datetime_val = datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")

        partitioned_df.write.format("mongodb").mode("append") \
            .option("uri", mongodb_write_uri).option("spark.mongodb.write.connection.uri", mongodb_write_uri). \
            option("spark.mongodb.output.uri", mongodb_write_uri). \
            option("connection.uri", mongodb_write_uri). \
            option("comment", f"Comment {datetime_val}"). \
            option("spark.mongodb.write.database", "aws_emr_pyspark_poc_db"). \
            option("spark.mongodb.output.database", "aws_emr_pyspark_poc_db"). \
            option("spark.mongodb.write.convertJson", "any"). \
            option("spark.mongodb.output.convertJson", "any"). \
            option("spark.mongodb.write.collection", "trade_feeds"). \
            option("spark.mongodb.output.collection", "trade_feeds"). \
            option("spark.mongodb.output.idFieldList", "CustomerId,Trading_Date_YYYY,Trading_Date_MM,Trading_Date_DD"). \
            partitionBy(["CustomerId", "Trading_Date_YYYY", "Trading_Date_MM", "Trading_Date_DD"]). \
            save()

        LOGGER.info("COMPLETED save_stg_data_in_mongodb")
        return EtlExecResults.SUCCESS

    @log_time
    def read_from_mongodb_after_save(self, wf_ctx: EtlExecCtx, wf_task_ctx: EtlExecTaskCtx) -> int:
        mongodb_write_uri = AppConfigs.get_config(MONGODB_URI)

        datetime_val = datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")

        read_df = wf_ctx.etl_exec_engine_ctx.spark_session.read.format("mongodb") \
            .option("uri", mongodb_write_uri).option("spark.mongodb.input.connection.uri", mongodb_write_uri). \
            option("spark.mongodb.input.uri", mongodb_write_uri). \
            option("connection.uri", mongodb_write_uri). \
            option("inferSchema", False). \
            option("schema", stg2raw_schema_read). \
            option("comment", f"Comment READ {datetime_val}"). \
            option("spark.mongodb.read.database", "aws_emr_pyspark_poc_db"). \
            option("spark.mongodb.input.database", "aws_emr_pyspark_poc_db"). \
            option("spark.mongodb.read.collection", "trade_feeds"). \
            option("spark.mongodb.input.collection", "trade_feeds"). \
            option("partitioner", "com.mongodb.spark.sql.connector.read.partitioner.ShardedPartitioner"). \
            load()
        read_df.createOrReplaceTempView("Trade_Events")

        read_df_sql = wf_ctx.etl_exec_engine_ctx.spark_session.sql("SELECT * FROM Trade_Events WHERE CustomerId > 10")

        read_df_sql.show(truncate=False)

        return EtlExecResults.SUCCESS

    @log_time
    def execute_job(self, wf_ctx: EtlExecCtx, wf_task_ctx: EtlExecTaskCtx) -> int:
        LOGGER.info("ENTERED execute_job")

        self.op_create_pyspark_session(wf_ctx=wf_ctx, wf_task_ctx=wf_task_ctx)
        self.read_stg_data(wf_ctx=wf_ctx, wf_task_ctx=wf_task_ctx)
        self.save_stg_data_in_mongodb(wf_ctx=wf_ctx, wf_task_ctx=wf_task_ctx)
        self.read_from_mongodb_after_save(wf_ctx=wf_ctx, wf_task_ctx=wf_task_ctx)

        LOGGER.info("COMPLETED execute_job")
        return EtlExecResults.SUCCESS
