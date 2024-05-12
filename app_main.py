import argparse

from datalake_mgmt.common.configs.app_configs import AppConfigs
from datalake_mgmt.common.configs.config_keys import MONGODB_URI
from datalake_mgmt.common.etl_framework.dtos import EtlExecCtx, EtlExecTaskCtx
from datalake_mgmt.common.etl_framework.interfaces import EtlHandler
from datalake_mgmt.util.etl_objects_factory import EtlObjectsFactory

import os

this_script_dir = str(os.path.dirname(os.path.realpath(__file__)))

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("app-log.log"),
        logging.StreamHandler()
    ]
)


def initialize_default_configs():
    # Configure the MongoDB connection

    # OThers Optionis tried but did not work local mongodb cluster
    mongodb_uri = "mongodb://admin:password@aws-emr-pyspark-mongo-node1:47017,aws-emr-pyspark-mongo-node2:47018," \
                  "aws-emr-pyspark-mongo-node3:47019/aws_emr_pyspark_db.trade_events?replicaSet=rs0"

    # Example mongodb_uri = "mongodb+srv://<username>:<password>@<cluster_name>.<other_identifier>.mongodb.net
    # /?retryWrites=true" \ "&w=majority&appName=Cluster0"
    mongodb_uri = os.getenv("MONGODB_ATLAS_CONNECTION_URL")

    AppConfigs.add_or_update_config(MONGODB_URI, mongodb_uri)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog='AWS EMR PySpark Datalake Management',
        description='Ingests Data From Staging DataZone to Raw DataZone and '
                    'Performs analytics on Raw Data')
    parser.add_argument('--which_etl', help="Stg2Raw Or Raw2Analytics", required=False)
    parser.add_argument('--stg2raw_dataset', help="Stg2Raw Or Raw2Analytics", required=False)

    args = parser.parse_args()

    which_etl = str(args.which_etl)
    print(f"which_etl INPUT [{which_etl}]")

    stg2raw_dataset = str(args.stg2raw_dataset)
    print(f"stg2raw_dataset INPUT [{stg2raw_dataset}]")

    if stg2raw_dataset is None or stg2raw_dataset == "None" or which_etl == "":
        stg2raw_dataset = f"{this_script_dir}/datasets/staging_data/2024-05-12"

    if which_etl is None or which_etl == "None" or which_etl == "":
        which_etl = "Stg2Raw"

    print(f"which_etl [{which_etl}]")
    print(f"stg2raw_dataset [{stg2raw_dataset}]")

    initialize_default_configs()

    etl_handler: EtlHandler = EtlObjectsFactory.get_etl_handler_by_name(which_etl)
    wf_ctx, wf_task_ctx = etl_handler.initialize_perform_etl(input_ctx={})
    wf_task_ctx.stg2raw_dataset_location = stg2raw_dataset

    wf_ctx: EtlExecCtx = wf_ctx
    wf_task_ctx: EtlExecTaskCtx = wf_task_ctx

    etl_handler.perform_etl(wf_ctx=wf_ctx, wf_task_ctx=wf_task_ctx)
