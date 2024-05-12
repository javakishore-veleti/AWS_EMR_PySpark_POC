import logging

from datalake_mgmt.common.constants.etl_exec_results import EtlExecResults
from datalake_mgmt.common.etl_framework.dtos import EtlExecCtx, EtlExecTaskCtx

LOGGER = logging.getLogger(__file__)


class EtlHandler:

    def __init__(self, bean_id: str = "EtlHandler", logger: logging.Logger = LOGGER):
        self.BEAN_ID = bean_id
        self.LOGGER = logger

    # noinspection PyMethodMayBeStatic
    def initialize_perform_etl(self, input_ctx: dict = None) -> (EtlExecCtx, EtlExecTaskCtx):
        return None, None

    def perform_etl(self, wf_ctx:EtlExecCtx, wf_task_ctx:EtlExecTaskCtx) -> int:
        return EtlExecResults.SUCCESS
