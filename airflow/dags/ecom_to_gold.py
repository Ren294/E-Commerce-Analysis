from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Ecom_from_Silver_to_Gold_DataWarehouse_with_Hive',
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
        command=f"""/opt/spark/bin/spark-submit {script_path} \
          --master spark://spark-master:7077""",
        dag=dag
    )


check_and_create_database_tables = SSHOperator(
    task_id='check_and_create_database_tables',
    ssh_hook=ssh_hook_spark,
    command="""/opt/spark/bin/spark-sql \
      -f /opt/spark-apps/toGold/ecom/createDB.sql""",
    dag=dag
)

check_data_in_silver_ecom = SSHOperator(
    task_id='check_data_in_silver_ecom',
    ssh_hook=ssh_hook_hdfs,
    command="""
    if /usr/local/hadoop/bin/hdfs dfs -count /data/silver/ecom | awk '{print $2}' | grep -q '^0$'; then
        echo "No data found in /data/silver/ecom"
        exit 1
    else
        echo "Data found in /data/silver/ecom"
    fi
    """,
    dag=dag
)
check_data_in_silver_marketing = SSHOperator(
    task_id='check_data_in_silver_marketing',
    ssh_hook=ssh_hook_hdfs,
    command="""
    if /usr/local/hadoop/bin/hdfs dfs -count /data/silver/marketing | awk '{print $2}' | grep -q '^0$'; then
        echo "No data found in /data/silver/marketing"
        exit 1
    else
        echo "Data found in /data/silver/marketing"
    fi
    """,
    dag=dag
)
dir = "/opt/spark-apps/toGold/ecom"

run_dim_customers = create_ssh_task(
    'run_dim_customers', f'{dir}/gold_dim_customers.py')
run_dim_geolocation = create_ssh_task(
    'run_dim_geolocation', f'{dir}/gold_dim_geolocation.py')
run_dim_product_category = create_ssh_task(
    'run_dim_product_category', f'{dir}/gold_dim_productCategory.py')
run_dim_products = create_ssh_task(
    'run_dim_products', f'{dir}/gold_dim_products.py')
run_dim_sellers = create_ssh_task(
    'run_dim_sellers', f'{dir}/gold_dim_sellers.py')
run_fact_order_item = create_ssh_task(
    'run_fact_order_item', f'{dir}/gold_fact_orderItem.py')
run_fact_orders = create_ssh_task(
    'run_fact_orders', f'{dir}/gold_fact_orders.py')

# run_fact_closed_deals = create_ssh_task(
#     'run_fact_closed_deals', f'{dir}/gold_fact_closed_deals.py')

run_dim_dates = create_ssh_task(
    'run_dim_dates', f'{dir}/gold_dim_dates.py')
run_dim_mql = create_ssh_task(
    'run_dim_mql', f'{dir}/gold_dim_mql.py')
run_dim_orders = create_ssh_task(
    'run_dim_orders', f'{dir}/gold_dim_orders.py')

run_fact_closed_deals = SSHOperator(
    task_id='run_fact_closed_deals',
    ssh_hook=ssh_hook_spark,
    command=f'''/opt/spark/bin/spark-submit \
          /opt/spark-apps/toGold/ecom/gold_fact_closed_deals.py \
          --master spark://spark-master:7077\
          --conf "spark.driver.extraJavaOptions=-XX:+UseG1GC" \
          --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC" \
          --conf "spark.sql.autoBroadcastJoinThreshold=-1"''',
    dag=dag
)
[check_data_in_silver_ecom,
    check_data_in_silver_marketing] >> check_and_create_database_tables
check_and_create_database_tables >> [run_dim_dates, run_dim_mql,
                                     run_dim_customers, run_dim_geolocation, run_dim_product_category, run_dim_products, run_dim_sellers, run_dim_orders]
run_dim_dates >> run_dim_customers >> run_dim_geolocation >> run_dim_product_category >> run_dim_products >> run_dim_sellers >> run_dim_mql >> run_dim_orders
[run_dim_dates, run_dim_customers, run_dim_geolocation, run_dim_product_category,
    run_dim_products, run_dim_sellers] >> run_fact_order_item
[run_dim_dates, run_dim_customers, run_dim_geolocation,
    run_dim_products] >> run_fact_orders
[run_dim_dates, run_dim_customers, run_dim_geolocation, run_dim_product_category,
    run_dim_products, run_dim_sellers, run_dim_mql, run_dim_orders] >> run_fact_closed_deals

run_fact_order_item >> run_fact_orders >> run_fact_closed_deals
