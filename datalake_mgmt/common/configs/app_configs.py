from datalake_mgmt.common.configs.config_keys import MONGODB_URI


class AppConfigs:

    CONFIGS_MAP = {}

    @staticmethod
    def add_or_update_config(key: str, value):
        AppConfigs.CONFIGS_MAP.update({key: value})

    @staticmethod
    def get_config(key: str):
        if key in AppConfigs.CONFIGS_MAP:
            return AppConfigs.CONFIGS_MAP[key]
        return None

    @staticmethod
    def get_config_as_str(key: str):
        if key in AppConfigs.CONFIGS_MAP:
            return str(AppConfigs.CONFIGS_MAP[key])
        return None

