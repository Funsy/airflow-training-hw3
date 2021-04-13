from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataProcHiveOperator

USERNAME = 'ashanin'

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2013, 1, 1, 0, 0, 0)
}

dag = DAG(
    USERNAME + '_data_lake_etl',
    default_args=default_args,
    description='Data Lake ETL tasks',
    schedule_interval="0 0 1 1 *",
)

ods_billing = DataProcHiveOperator(
    task_id='ods_billing',
    dag=dag,
    query="""
        INSERT OVERWRITE TABLE ashanin.ods_billing PARTITION (year='{{ execution_date.year }}') 
        SELECT user_id, cast(billing_period as TIMESTAMP), service, tariff, cast(sum as INT), cast(created_at as TIMESTAMP) 
        FROM ashanin.stg_billing WHERE year(created_at) = {{ execution_date.year }};
    """,
    cluster_name='cluster-dataproc',
    job_name=USERNAME + '_ods_billing_{{ execution_date.year }}_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
)

ods_issue = DataProcHiveOperator(
    task_id='ods_issue',
    dag=dag,
    query="""
        INSERT OVERWRITE TABLE ashanin.ods_issue PARTITION (year='{{ execution_date.year }}') 
        SELECT cast(user_id as INT), cast(start_time as TIMESTAMP), cast(end_time as TIMESTAMP), title, description, service 
        FROM ashanin.stg_issue WHERE year(start_time) = {{ execution_date.year }};
    """,
    cluster_name='cluster-dataproc',
    job_name=USERNAME + '_ods_issue_{{ execution_date.year }}_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
)

ods_payment = DataProcHiveOperator(
    task_id='ods_payment',
    dag=dag,
    query="""
        INSERT OVERWRITE TABLE ashanin.ods_payment PARTITION (year='{{ execution_date.year }}') 
        SELECT user_id, pay_doc_type, pay_doc_num, account, phone, cast(billing_period as TIMESTAMP), cast(pay_date as TIMESTAMP), cast(sum as DECIMAL(10,2)) 
        FROM ashanin.stg_payment WHERE year(pay_date) = {{ execution_date.year }};
    """,
    cluster_name='cluster-dataproc',
    job_name=USERNAME + '_ods_payment_{{ execution_date.year }}_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
)

ods_traffic = DataProcHiveOperator(
    task_id='ods_traffic',
    dag=dag,
    query="""
        INSERT OVERWRITE TABLE ashanin.ods_traffic PARTITION (year='2020') 
        SELECT user_id, cast(`timestamp` as TIMESTAMP), device_id, device_ip_addr, bytes_sent, bytes_recieved 
        FROM ashanin.stg_traffic WHERE year(from_unixtime(cast(`timestamp`/1000 as BIGINT))) = 2020;
    """,
    cluster_name='cluster-dataproc',
    job_name=USERNAME + '_ods_traffic_{{ execution_date.year }}_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
)

ods_bytes_received_per_user = DataProcHiveOperator(
    task_id='ods_bytes_received_per_user',
    dag=dag,
    query="""
        INSERT OVERWRITE TABLE ashanin.ods_billing PARTITION (year='{{ execution_date.year }}') 
        SELECT * from ashanin.dm_bytes_received WHERE year(created_at) = {{ execution_date.year }};
    """,
    cluster_name='cluster-dataproc',
    job_name=USERNAME + '_ods_bytes_received_per_user_{{ execution_date.year }}_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
)

ods_billing, ods_issue, ods_payment, ods_traffic >> ods_bytes_received_per_user
