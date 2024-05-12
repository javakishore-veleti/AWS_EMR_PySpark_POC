from datalake_mgmt.common.etl_framework.interfaces import EtlHandler
from datalake_mgmt.staging_to_raw.stg2raw_etl_handler import Stg2RawEtlHandler


class EtlObjectsFactory:

    @staticmethod
    def get_etl_handler_by_name(etl_name: str) -> EtlHandler:
        if etl_name == "Stg2Raw":
            return Stg2RawEtlHandler()