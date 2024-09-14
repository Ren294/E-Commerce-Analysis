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
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Ecom_from_Bronze_to_Silver_using_Spark',
    default_args=default_args,
    description='Run PySpark jobs using SSH in a specific order',
    schedule_interval='@daily',
    catchup=False
)

ssh_hook_spark = SSHHook(ssh_conn_id='spark_server', cmd_timeout=None)
ssh_hook_hdfs = SSHHook(ssh_conn_id='hdfs_server', cmd_timeout=None)


def create_spark_task(task_id, script_name):
    return SSHOperator(
        task_id=task_id,
        ssh_hook=ssh_hook_spark,
        command=f"""
          /opt/spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            /opt/spark-apps/toSilver/ecom/{script_name}.py
          """,
        dag=dag
    )


check_data_in_hdfs = SSHOperator(
    task_id='check_data_in_hdfs',
    ssh_hook=ssh_hook_hdfs,
    command="""
    if /usr/local/hadoop/bin/hdfs dfs -count /data/bronze/ecom | awk '{print $2}' | grep -q '^0$'; then
        echo "No data found in /data/bronze/ecom"
        exit 1
    else
        echo "Data found in /data/bronze/ecom"
    fi
    """,
    dag=dag
)

run_silver_customer = create_spark_task(
    'run_silver_customer', 'silver_customer')
run_silver_geolocation = create_spark_task(
    'run_silver_geolocation', 'silver_geolocation')
run_silver_sellers = create_spark_task('run_silver_sellers', 'silver_sellers')
run_silver_products = create_spark_task(
    'run_silver_products', 'silver_products')
run_silver_product_category_name_translations = create_spark_task(
    'run_silver_product_category_name_translations', 'silver_product_category_name_translations')
run_silver_orders = create_spark_task('run_silver_orders', 'silver_orders')
run_silver_order_items = create_spark_task(
    'run_silver_order_items', 'silver_order_id')
run_silver_order_payments = create_spark_task(
    'run_silver_order_payments', 'silver_order_payments')
run_silver_order_reviews = create_spark_task(
    'run_silver_order_reviews', 'silver_order_reviews')

check_create_hdfs_directory = SSHOperator(
    task_id='check_create_hdfs_directory',
    ssh_hook=ssh_hook_hdfs,
    command="""
    /usr/local/hadoop/bin/hdfs dfs -test -d /data/silver/ecom || /usr/local/hadoop/bin/hdfs dfs -mkdir -p /data/silver/ecom
    /usr/local/hadoop/bin/hdfs dfs -chmod -R 755 /data/silver/ecom
    """,
    dag=dag
)

check_data_in_hdfs >> check_create_hdfs_directory >> run_silver_customer

run_silver_customer >> [run_silver_geolocation, run_silver_sellers]
run_silver_geolocation >> run_silver_products
run_silver_sellers >> [run_silver_products, run_silver_orders]

run_silver_products >> [
    run_silver_product_category_name_translations, run_silver_order_items]
run_silver_product_category_name_translations >> run_silver_orders
run_silver_orders >> [run_silver_order_payments, run_silver_order_reviews]
run_silver_order_items >> run_silver_order_reviews
run_silver_order_reviews >> run_silver_order_payments
