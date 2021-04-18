from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataProcHiveOperator

USERNAME = 'ashanin'

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2012, 1, 1, 0, 0, 0)
}

dag = DAG(
    USERNAME + '_dwh_etl',
    default_args=default_args,
    description='DWH ETL tasks',
    schedule_interval="0 0 1 1 *",
)

dag1 = DataProcHiveOperator(
    task_id='ods_billing',
    dag=dag,
    query="""
        INSERT OVERWRITE TABLE ashanin.ods_billing PARTITION (year='{{ execution_date.year }}') 
        SELECT user_id, cast(concat(billing_period, '-01') as DATE), service, tariff, cast(sum as INT), cast(created_at as DATE) 
        FROM ashanin.stg_billing 
        WHERE year(created_at) = '{{ execution_date.year }}';
    """,
    cluster_name='cluster-dataproc',
    job_name=USERNAME + '_ods_billing_{{ execution_date.year }}_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
)

dag2 = DataProcHiveOperator(
    task_id='ods_issue',
    dag=dag,
    query="""
        INSERT OVERWRITE TABLE ashanin.ods_issue PARTITION (year='{{ execution_date.year }}') 
        SELECT cast(user_id as INT), cast(start_time as TIMESTAMP), cast(end_time as TIMESTAMP), title, description, service 
        FROM ashanin.stg_issue 
        WHERE year(start_time) = '{{ execution_date.year }}';
    """,
    cluster_name='cluster-dataproc',
    job_name=USERNAME + '_ods_issue_{{ execution_date.year }}_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
)

dag1 >> dag2
