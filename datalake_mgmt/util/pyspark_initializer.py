# Import the SparkSession class
from pyspark.sql import SparkSession

from datalake_mgmt.common.constants.etl_exec_results import EtlExecResults
from datalake_mgmt.common.etl_framework.dtos import EtlExecTaskCtx, EtlExecCtx
from datalake_mgmt.util.times_monitors import log_time


class PySparkInitializer:

    @log_time
    @staticmethod
    def create_pyspark_session(wf_ctx: EtlExecCtx, wf_task_ctx: EtlExecTaskCtx) -> int:
        spark_session = SparkSession.builder.getOrCreate()
        print(spark_session.sparkContext.getConf().getAll())

        wf_ctx.etl_exec_engine_ctx.spark_session = spark_session
        return EtlExecResults.SUCCESS
