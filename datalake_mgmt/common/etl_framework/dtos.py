from pyspark.sql import SparkSession, DataFrame


class EtlExecEngineCtx:
    def __init__(self):
        self.ctx_data = {}
        self.spark_session: SparkSession = None
        self.engine_type: str = "PySpark"


class EtlExecEngineTaskCtx:
    def __init__(self):
        self.ctx_data = {}
        self.latest_df: DataFrame = None


class EtlRequestCtx:
    def __init__(self):
        self.ctx_data = {}


class EtlExecCtx:
    def __init__(self):
        self.ctx_data = {}
        self.req_ctx: EtlRequestCtx = None
        self.etl_exec_engine_ctx: EtlExecEngineCtx = EtlExecEngineCtx()


class EtlExecTaskCtx:
    def __init__(self):
        self.ctx_data = {}
        self.stg2raw_dataset_location: str = ""
        self.etl_exec_engine_task_ctx: EtlExecEngineTaskCtx = EtlExecEngineTaskCtx()

    def initialize_etl_exec_engine_task_ctx(self):
        self.etl_exec_engine_task_ctx: EtlExecEngineTaskCtx = EtlExecEngineTaskCtx()


class St2RawEtlExecCtx(EtlExecCtx):

    def __init__(self):
        super().__init__()


class St2RawEtlExecTaskCtx(EtlExecTaskCtx):

    def __init__(self):
        super().__init__()
