from airflow import DAG
#from airflow.operators.bash import BashOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="generatecar_workflow_dag",
    default_args=default_args,
    #schedule_interval=None,
    catchup=False,
    description="Generate CAR data and upload to S3",
) as dag:

    generate_car = BashOperator(
        task_id="generate_car",
        bash_command="python /home/smcphail/Projects/AML_KYC/generateCAR.py {{ ds }}",
    )

    upload_car_to_s3 = BashOperator(
        task_id="upload_car_to_s3",
        bash_command="python /home/smcphail/Projects/AML_KYC/uploadCARtoS3.py {{ ds }}",
    )

    generate_car >> upload_car_to_s3
