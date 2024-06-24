from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow'
}

with DAG(
    dag_id='ETL_dag_hijo',
    default_args=default_args,
    schedule_interval="@daily",
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),
    catchup=False,
    tags=['process']
) as dag:

    comienzo = DummyOperator(
        task_id='Comienzo',
    )

    process_data = BashOperator(
        task_id='process_data',
        bash_command='ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/transform_car_rentals.py',
        dag=dag
    )

    fin = DummyOperator(
        task_id='fin',
    )

    comienzo >> process_data >> fin

if __name__ == "__main__":
    dag.cli()