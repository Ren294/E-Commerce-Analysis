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
    'Log_from_Silver_to_Gold_with_Hive',
    default_args=default_args,
    description='Run PySpark ETL scripts in sequence using SSHOperator and HiveOperator',
    schedule_interval='@daily',
    start_date=datetime.now(),
    catchup=False,
)
ssh_hook_spark = SSHHook(ssh_conn_id='spark_server', cmd_timeout=None)
ssh_hook_hdfs = SSHHook(ssh_conn_id='hdfs_server', cmd_timeout=None)


def create_ssh_task(task_id, script_path):
    return SSHOperator(
        task_id=task_id,
        ssh_hook=ssh_hook_spark,
        command=f"""/opt/spark/bin/spark-submit \
          --master spark://spark-master:7077 \
          --packages io.delta:delta-core_2.12:2.0.0,io.delta:delta-storage:2.0.0 \
          {script_path}
          """,
        dag=dag
    )


check_and_create_database_tables = SSHOperator(
    task_id='check_and_create_database_tables',
    ssh_hook=ssh_hook_spark,
    command="""/opt/spark/bin/spark-sql \
      -f /opt/spark-apps/toGold/log/createDB.sql""",
    dag=dag
)

check_data_in_silver_logs = SSHOperator(
    task_id='check_data_in_silver_logs',
    ssh_hook=ssh_hook_hdfs,
    command="""
    if /usr/local/hadoop/bin/hdfs dfs -count /data/silver/log | awk '{print $2}' | grep -q '^0$'; then
        echo "No data found in /data/silver/log"
        exit 1
    else
        echo "Data found in /data/silver/log"
    fi
    """,
    dag=dag
)

check_data_in_silver_clicks = SSHOperator(
    task_id='check_data_in_silver_clicks',
    ssh_hook=ssh_hook_hdfs,
    command="""
    if /usr/local/hadoop/bin/hdfs dfs -count /data/silver/click | awk '{print $2}' | grep -q '^0$'; then
        echo "No data found in /data/silver/click"
        exit 1
    else
        echo "Data found in /data/silver/click"
    fi
    """,
    dag=dag
)

dir = "/opt/spark-apps/toGold/log"

get_clickStream = create_ssh_task(
    'get_clickStream', f'{dir}/gold_clickstream.py')

get_log = create_ssh_task(
    'get_log', f'{dir}/gold_log.py')

start_dag = DummyOperator(
    task_id='sstart_dag',
    dag=dag,
)

end_dag = DummyOperator(
    task_id='end_dag',
    dag=dag,
)
start_dag >> [check_data_in_silver_logs, check_data_in_silver_clicks]
[check_data_in_silver_logs, check_data_in_silver_clicks] >> check_and_create_database_tables >> [
    get_clickStream, get_log]
get_clickStream >> get_log >> end_dag
