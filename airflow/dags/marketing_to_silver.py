from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Marketing_from_Bronze_to_Silver_using_Spark',
    default_args=default_args,
    description='A DAG to process marketing data with PySpark',
    schedule_interval='@daily',
    catchup=False
)

ssh_hook_spark = SSHHook(ssh_conn_id='spark_server', cmd_timeout=None)
ssh_hook_hdfs = SSHHook(ssh_conn_id='hdfs_server', cmd_timeout=None)

pyspark_closed_deals_command = '/opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-apps/toSilver/marketing/silver_closed_deals.py'
pyspark_marketing_qualified_leads_command = '/opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-apps/toSilver/marketing/silver_marketing_qualified_leads.py'

check_data_in_hdfs = SSHOperator(
    task_id='check_data_in_hdfs',
    ssh_hook=ssh_hook_hdfs,
    command="""
    if /usr/local/hadoop/bin/hdfs dfs -count /data/bronze/marketing | awk '{print $2}' | grep -q '^0$'; then
        echo "No data found in /data/bronze/marketing"
        exit 1
    else
        echo "Data found in /data/bronze/marketing"
    fi
    """,
    dag=dag
)

run_sliver_closed_deals = SSHOperator(
    task_id='run_sliver_closed_deals',
    ssh_hook=ssh_hook_spark,
    command=pyspark_closed_deals_command,
    dag=dag
)

run_sliver_marketing_qualified_leads = SSHOperator(
    task_id='run_sliver_marketing_qualified_leads',
    ssh_hook=ssh_hook_spark,
    command=pyspark_marketing_qualified_leads_command,
    dag=dag
)
check_create_hdfs_directory = SSHOperator(
    task_id='check_create_hdfs_directory',
    ssh_hook=ssh_hook_hdfs,
    command="""
    /usr/local/hadoop/bin/hdfs dfs -test -d /data/silver/marketing || /usr/local/hadoop/bin/hdfs dfs -mkdir -p /data/silver/marketing
    /usr/local/hadoop/bin/hdfs dfs -chmod -R 755 /data/silver/marketing
    """,
    dag=dag
)


check_data_in_hdfs >> check_create_hdfs_directory >> [run_sliver_closed_deals,
                                                      run_sliver_marketing_qualified_leads]

run_sliver_closed_deals >> run_sliver_marketing_qualified_leads
