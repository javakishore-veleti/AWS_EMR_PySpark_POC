from overrides import override

from datalake_mgmt.common.constants.etl_exec_results import EtlExecResults
from datalake_mgmt.common.etl_framework.dtos import St2RawEtlExecCtx, St2RawEtlExecTaskCtx, EtlExecCtx, EtlExecTaskCtx
from datalake_mgmt.common.etl_framework.interfaces import EtlHandler
import logging

from datalake_mgmt.staging_to_raw.etl_jobs.stg2raw_etl_job import Stg2RawEtlJob

LOGGER = logging.getLogger(__file__)
BEAN_ID = "Stg2RawEtlHandler"


class Stg2RawEtlHandler(EtlHandler):

    def __init__(self):
        super().__init__(BEAN_ID, LOGGER)

    @override
    def initialize_perform_etl(self, input_ctx: dict = None) -> (EtlExecCtx, EtlExecTaskCtx):
        return EtlExecCtx(), EtlExecTaskCtx()

    @override
    def perform_etl(self, wf_ctx:EtlExecCtx, wf_task_ctx:EtlExecTaskCtx) -> int:
        wf_task_ctx.initialize_etl_exec_engine_task_ctx()
        Stg2RawEtlJob().execute_job(wf_ctx=wf_ctx, wf_task_ctx=wf_task_ctx)
        return EtlExecResults.SUCCESS
