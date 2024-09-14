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
    'Import_Ecom_data_to_Bronze_State_using_SQOOP',
    default_args=default_args,
    description='Incremental Sqoop import for e-commerce tables into HDFS',
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
            --query 'SELECT * FROM "ecommerce"."{table_name}" WHERE $CONDITIONS' \
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


import_customers = create_incremental_sqoop_task(
    'customers',
    '/data/bronze/ecom/customers',
    'customer_id',
)

import_geolocation = create_incremental_sqoop_task(
    'geolocation',
    '/data/bronze/ecom/geolocation',
    'geolocation_zip_code_prefix',
)

import_order_items = create_incremental_sqoop_task(
    'order_items',
    '/data/bronze/ecom/order_items',
    'order_item_id',
)

import_order_payments = create_incremental_sqoop_task(
    'order_payments',
    '/data/bronze/ecom/order_payments',
    'payment_sequential',
)

import_order_reviews = create_incremental_sqoop_task(
    'order_reviews',
    '/data/bronze/ecom/order_reviews',
    'review_id',
)

import_orders = create_incremental_sqoop_task(
    'orders',
    '/data/bronze/ecom/orders',
    'order_id',
)

import_products = create_incremental_sqoop_task(
    'products',
    '/data/bronze/ecom/products',
    'product_id',
)

import_sellers = create_incremental_sqoop_task(
    'sellers',
    '/data/bronze/ecom/sellers',
    'seller_id',
)

import_product_category_name_translations = create_incremental_sqoop_task(
    'product_category_name_translations',
    '/data/bronze/ecom/product_category_name_translations',
    'product_category_name',
)

check_create_hdfs_directory = SSHOperator(
    task_id='check_create_hdfs_directory',
    ssh_hook=ssh_hook_hdfs,
    command="""
    /usr/local/hadoop/bin/hdfs dfs -test -d /data/bronze/ecom || /usr/local/hadoop/bin/hdfs dfs -mkdir -p /data/bronze/ecom
    /usr/local/hadoop/bin/hdfs dfs -chmod -R 755 /data/bronze/ecom
    """,
    dag=dag
)

export_conditions_task = create_export_conditions_task()

check_create_hdfs_directory >> export_conditions_task >> [
    import_geolocation, import_sellers, import_customers, import_product_category_name_translations, import_products, import_orders, import_order_items, import_order_payments, import_order_reviews]
import_geolocation >> import_sellers >> import_customers
import_customers >> [
    import_product_category_name_translations, import_products]
import_products >> import_orders >> [
    import_order_items, import_order_payments, import_order_reviews]

import_order_items >> import_order_payments >> import_order_reviews
