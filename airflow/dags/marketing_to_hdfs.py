from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.models import Variable
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=10)
}

dag = DAG(
    'Import_Marketing_data_to_Bronze_State_using_SQOOP',
    default_args=default_args,
    description='Incremental Sqoop import for marketing tables into HDFS',
    schedule_interval='@daily',
    catchup=False
)

ssh_hook_sqoop = SSHHook(ssh_conn_id='sqoop_server', cmd_timeout=None)
ssh_hook_hdfs = SSHHook(ssh_conn_id='hdfs_server', cmd_timeout=None)


def create_incremental_sqoop_task(table_name, target_dir, split_by):
    last_value = Variable.get(
        f'last_value_{table_name}', default_var='1970-01-01')

    return SSHOperator(
        task_id=f'sqoop_incremental_import_{table_name}',
        ssh_hook=ssh_hook_sqoop,
        command=f"""
          /usr/lib/sqoop/bin/sqoop import \
            --connect jdbc:postgresql://postgres_ecom:5432/ecom \
            --username postgres \
            --password postgres \
            --query 'SELECT * FROM "marketing"."{table_name}" WHERE $CONDITIONS' \
            --target-dir {target_dir} \
            --split-by {split_by} \
            --incremental append \
            --check-column id \
            --fields-terminated-by ',' \
            --lines-terminated-by '\n'
        """,
        dag=dag
    )


def create_export_conditions_task():
    return SSHOperator(
        task_id='export_conditions',
        ssh_hook=ssh_hook_sqoop,
        command='export CONDITIONS="1=1"',
        dag=dag,
    )


import_closed_deals = create_incremental_sqoop_task(
    'closed_deals',
    '/data/bronze/marketing/closed_deals',
    'mql_id',
)

import_marketing_qualified_leads = create_incremental_sqoop_task(
    'marketing_qualified_leads',
    '/data/bronze/marketing/marketing_qualified_leads',
    'mql_id',
)

check_create_hdfs_directory = SSHOperator(
    task_id='check_create_hdfs_directory',
    ssh_hook=ssh_hook_hdfs,
    command="""
    /usr/local/hadoop/bin/hdfs dfs -test -d /data/bronze/marketing || /usr/local/hadoop/bin/hdfs dfs -mkdir -p /data/bronze/marketing
    /usr/local/hadoop/bin/hdfs dfs -chmod -R 755 /data/bronze/marketing
    """,
    dag=dag
)

export_conditions_task = create_export_conditions_task()

check_create_hdfs_directory >> export_conditions_task >> [
    import_marketing_qualified_leads, import_closed_deals]

import_marketing_qualified_leads >> import_closed_deals
