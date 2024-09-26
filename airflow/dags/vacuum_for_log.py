from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Log_Vacuum_Clickstream_Log',
    default_args=default_args,
    description='Run vacuum scripts for Clickstream and Log data using SSHOperator',
    schedule_interval='@weekly',
    start_date=datetime.now(),
    catchup=False,
)

ssh_hook_spark = SSHHook(ssh_conn_id='spark_server', cmd_timeout=None)


def create_check_task(task_id, table_name):
    return SSHOperator(
        task_id=task_id,
        ssh_hook=ssh_hook_spark,
        command=f"""
        count=$(/opt/spark/bin/spark-sql -e "SELECT COUNT(*) FROM {table_name}")
        if [ "$count" -eq 0 ]; then
            echo "Table {table_name} is empty."
            exit 0  # Exit as successful
        else
            echo "Table {table_name} has records."
            exit 0  # Exit as successful
        fi
        """,
        dag=dag
    )


def create_ssh_vacuum_task(task_id, script_path):
    return SSHOperator(
        task_id=task_id,
        ssh_hook=ssh_hook_spark,
        command=f"""/opt/spark/bin/spark-submit \
          --master spark://spark-master:7077 \
          --packages io.delta:delta-core_2.12:2.0.0,io.delta:delta-storage:2.0.0 \
          {script_path}""",
        dag=dag
    )


check_clickstream = create_check_task(
    'check_clickstream', 'logs.Ecom_clickstream')
check_log = create_check_task('check_log', 'logs.Ecom_log')

vacuum_clickstream = create_ssh_vacuum_task(
    'vacuum_clickstream', '/opt/spark-apps/vacuum/log/vacuum_clickstream.py')
vacuum_log = create_ssh_vacuum_task(
    'vacuum_log', '/opt/spark-apps/vacuum/log/vacuum_log.py')

start_vacuum_dag = DummyOperator(
    task_id='start_dag',
    dag=dag,
)

end_vacuum_dag = DummyOperator(
    task_id='end_dag',
    dag=dag,
)

start_vacuum_dag >> [check_clickstream, check_log]

check_clickstream >> vacuum_clickstream >> vacuum_log >> end_vacuum_dag
check_log >> vacuum_log
