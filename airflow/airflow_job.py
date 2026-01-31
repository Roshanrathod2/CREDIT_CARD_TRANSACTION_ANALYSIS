from airflow import DAG
from datetime import datetime,timedelta
from airflow.utils.dates import days_ago
from airflow.provider.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.provider.google.cloud.transfers.gcs_to_gcs import GCSTOGCSOperator
from airflow.utils.trigger_rule import TriggerRule

default_args={
    'owner':'airflow',
    "start_date":days_ago(1),
    'depend_on_past':False,
    'retries':1,
    'retry_delay':timedelta(minutes=5)
}

with DAG(
    dag_id='dag_id1',
    description="credit card transaction analysis",
    default_args=default_args,
    schedule_interval="0 5 * * *",
) as dag:
    
    #GCS CONFIG
    gcs_bucket="avd-bucket-credit-card-analysis"
    file_pattern="transactions/transactions_"
    source_suffix="transactions/"
    archive_suffix="archive/"

    #Generate unique batch IDs
    batch_id_users=f"load-users-batch-{str(uuid.uuid4()[:8])}"
    batch_id_txns=f"credit-card-batch-{str(uuid.uuid4()[:8])}"

    #dataproc Job1: load users data to bq
    load_user_task=DataprocCreateBatchOperator(
        task_id="load_users_to_bq",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": f"gs://{gcs_bucket}/spark_job/load_users_to_bq.py"
            },
            "runtime_config": {
                "version": "2.2.61",
            },
            "environment_config": {
                "execution_config": {
                    "service_account": "90174084486-compute@developer.gserviceaccount.com",
                    "network_uri": "projects/solid-box-479208-i4/global/networks/default",
                    "subnetwork_uri": "projects/solid-box-479208-i4/regions/us-central1/subnetworks/default",
                }
            },
 
        },
        batch_id=batch_id_users,
        project_id="solid-box-479208-i4",
        region="us-central1",
        gcp_conn_id="google_cloud_default",

    )

    #dataproc job2: process transaction , "version":"2.2",
    process_txns_task=DataprocCreateBatchOperator(
        task_id="run_credit_card_processing_job",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": f"gs://{gcs_bucket}/spark_job/spark_job.py"
            },
            "runtime_config": {
                "version": "2.2.61",
            },
            "environment_config": {
                "execution_config": {
                    "service_account": "90174084486-compute@developer.gserviceaccount.com",
                    "network_uri": "projects/solid-box-479208-i4/global/networks/default",
                    "subnetwork_uri": "projects/solid-box-479208-i4/regions/us-central1/subnetworks/default",
                }
            },
        },
        batch_id=batch_id_txns,
        project_id="solid-box-479208-i4",
        region="us-central1",
        gcp_conn_id="google_cloud_default",
    )

    #Archive processed files
    move_files_to_archive=GCSTOGCSOperator(
        task_id="move_file_to_archive",
        source_bucket=gcs_bucket,
        source_object=source_suffix,
        destination_bucket=gcs_bucket,
        destination_object=archive_suffix,
        move_object=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,

    )

    # DAG FLOW
    # load_user_task >> process_tranx_task >> move_file_to_archive
    process_txns_task >> move_files_to_archive