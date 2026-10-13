from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime, timedelta
import random

default_args = {
    'owner': 'junior_ai_engineer',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def check_data_volume(**kwargs):
    # Simuliert die Prüfung, ob viele Daten anfallen (Full) oder wenige (Incremental)
    volume = random.randint(1, 100)
    kwargs['ti'].xcom_push(key='data_volume', value=volume)
    return volume

def decide_branch(**kwargs):
    ti = kwargs['ti']
    volume = ti.xcom_pull(task_ids='check_data_volume', key='data_volume')
    if volume > 50:
        return 'branch_full_load'
    else:
        return 'branch_incremental_load'

def validate_redshift(**kwargs):
    # Hier würde ein SQL-Check stehen (z.B. mit PostgresOperator oder RedshiftDataOperator)
    print("Validiere Daten in Redshift... Datenqualitäts-Check bestanden!")
    return True

with DAG(
    'ecommerce_data_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['ecommerce', 'aws', 'glue', 'redshift'],
) as dag:

    # Task 1: Start
    start_pipeline = DummyOperator(task_id='start_pipeline')

    # Task 2: Check Volume
    check_data_volume = PythonOperator(
        task_id='check_data_volume',
        python_callable=check_data_volume,
        provide_context=True
    )

    # Task 3: Branching Logic
    branch_decision = BranchPythonOperator(
        task_id='branch_decision',
        python_callable=decide_branch,
        provide_context=True
    )

    # Task 4: Branch A (Full Load)
    branch_full_load = LambdaInvokeFunctionOperator(
        task_id='branch_full_load',
        function_name='extract_postgres_lambda',
        payload='{"load_type": "full"}',
        aws_conn_id='aws_default'
    )

    # Task 5: Branch B (Incremental Load)
    branch_incremental_load = LambdaInvokeFunctionOperator(
        task_id='branch_incremental_load',
        function_name='extract_postgres_lambda',
        payload='{"load_type": "incremental"}',
        aws_conn_id='aws_default'
    )

    # Task 6: Join Branches
    join_branches = DummyOperator(
        task_id='join_branches',
        trigger_rule='none_failed'
    )

    # Task 7: Trigger AWS Glue
    run_glue_etl = GlueJobOperator(
        task_id='run_glue_etl',
        job_name='ecommerce_glue_etl_job',
        iam_role_name='GlueRedshiftRole',
        script_args={
            '--S3_BUCKET': 'ecom-raw-data-bucket-[dein-name]',
            '--REDSHIFT_DB': 'ecom_dw',
            '--REDSHIFT_USER': 'admin',
            '--REDSHIFT_PASSWORD': 'DeinSicheresPasswort',
            '--REDSHIFT_URL': 'dein-cluster.redshift.amazonaws.com:5439'
        },
        aws_conn_id='aws_default',
        region_name='eu-central-1'
    )

    # Task 8: Validate Data
    validate_data = PythonOperator(
        task_id='validate_redshift_data',
        python_callable=validate_redshift
    )

    # DAG Flow
    start_pipeline >> check_data_volume >> branch_decision
    branch_decision >> [branch_full_load, branch_incremental_load] >> join_branches
    join_branches >> run_glue_etl >> validate_data
