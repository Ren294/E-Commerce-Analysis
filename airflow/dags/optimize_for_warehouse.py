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
    'Ecom_Optimization_DataWarehouse',
    default_args=default_args,
    description='Run optimization scripts for Delta tables in sequence using SSHOperator',
    schedule_interval='@weekly',
    start_date=datetime.now(),
    catchup=False,
)

ssh_hook_spark = SSHHook(ssh_conn_id='spark_server', cmd_timeout=None)


def create_ssh_task(task_id, script_path):
    return SSHOperator(
        task_id=task_id,
        ssh_hook=ssh_hook_spark,
        command=f"""/opt/spark/bin/spark-submit \
          --master spark://spark-master:7077 \
          --packages io.delta:delta-core_2.12:2.0.0,io.delta:delta-storage:2.0.0 \
          {script_path}""",
        dag=dag
    )


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
            exit 0  # Still exit as successful or change to exit 1 for failure
        fi
        """,
        dag=dag
    )


optimize_dir = "/opt/spark-apps/optimization/warehouse"

optimize_dim_customers = create_ssh_task('optimize_dim_customers',
                                         f'{optimize_dir}/optimize_dim_customers.py')
optimize_dim_dates = create_ssh_task(
    'optimize_dim_dates', f'{optimize_dir}/optimize_dim_dates.py')
optimize_dim_geolocation = create_ssh_task('optimize_dim_geolocation',
                                           f'{optimize_dir}/optimze_dim_geolocation.py')
optimize_dim_mql = create_ssh_task(
    'optimize_dim_mql', f'{optimize_dir}/optimize_dim_mql.py')
optimize_dim_orders = create_ssh_task(
    'optimize_dim_orders', f'{optimize_dir}/optimize_dim_orders.py')
optimize_dim_product_category = create_ssh_task('optimize_dim_product_category',
                                                f'{optimize_dir}/optimize_dim_productCategory.py')
optimize_dim_products = create_ssh_task(
    'optimize_dim_products', f'{optimize_dir}/optimize_dim_products.py')
optimize_dim_sellers = create_ssh_task(
    'optimize_dim_sellers', f'{optimize_dir}/optimize_dim_sellers.py')
optimize_fact_closed_deals = create_ssh_task('optimize_fact_closed_deals',
                                             f'{optimize_dir}/optimize_fact_closed_deals.py')
optimize_fact_order_item = create_ssh_task('optimize_fact_order_item',
                                           f'{optimize_dir}/opimize_fact_orderItem.py')
optimize_fact_orders = create_ssh_task(
    'optimize_fact_orders', f'{optimize_dir}/optimize_fact_orders.py')

check_dim_customers = create_check_task(
    'check_dim_customers', 'ecom.dim_customers')
check_dim_dates = create_check_task('check_dim_dates', 'ecom.dim_dates')
check_dim_geolocation = create_check_task(
    'check_dim_geolocation', 'ecom.dim_geolocation')
check_dim_mql = create_check_task('check_dim_mql', 'ecom.dim_mql')
check_dim_orders = create_check_task('check_dim_orders', 'ecom.dim_orders')
check_dim_product_category = create_check_task(
    'check_dim_product_category', 'ecom.dim_productCategory')
check_dim_products = create_check_task(
    'check_dim_products', 'ecom.dim_products')
check_dim_sellers = create_check_task('check_dim_sellers', 'ecom.dim_sellers')
check_fact_closed_deals = create_check_task(
    'check_fact_closed_deals', 'ecom.fact_closed_deals')
check_fact_order_item = create_check_task(
    'check_fact_order_item', 'ecom.fact_orderItem')
check_fact_orders = create_check_task('check_fact_orders', 'ecom.fact_orders')

start_dag = DummyOperator(
    task_id='sstart_dag',
    dag=dag,
)

end_dag = DummyOperator(
    task_id='end_dag',
    dag=dag,
)

# check_dim_dates >> optimize_dim_dates
# optimize_dim_dates >> [check_dim_customers, optimize_dim_customers]
# check_dim_customers >> optimize_dim_customers
# optimize_dim_customers >> [check_dim_geolocation, optimize_dim_geolocation]
# check_dim_geolocation >> optimize_dim_geolocation
# optimize_dim_geolocation >> [check_dim_mql, optimize_dim_mql]
# check_dim_mql >> optimize_dim_mql
# optimize_dim_mql >> [check_dim_orders, optimize_dim_orders]
# check_dim_orders >> optimize_dim_orders
# optimize_dim_orders >> [check_dim_product_category,
#                         optimize_dim_product_category]
# check_dim_product_category >> optimize_dim_product_category
# optimize_dim_product_category >> [check_dim_products, optimize_dim_products]
# check_dim_products >> optimize_dim_products
# optimize_dim_products >> [check_dim_sellers, optimize_dim_sellers]
# check_dim_sellers >> optimize_dim_sellers
# optimize_dim_sellers >> [check_fact_order_item, optimize_fact_order_item]
# check_fact_order_item >> optimize_fact_order_item
# optimize_fact_order_item >> [check_fact_orders, optimize_fact_orders]
# check_fact_orders >> optimize_fact_orders
# optimize_fact_orders >> [check_fact_closed_deals, optimize_fact_closed_deals]
# check_fact_closed_deals >> optimize_fact_closed_deals
start_dag >> check_dim_dates >> optimize_dim_dates
optimize_dim_dates >> check_dim_orders
optimize_dim_product_category >> [check_dim_customers,
                                  check_dim_geolocation, check_dim_products]
optimize_dim_dates >> [check_dim_customers,
                       check_dim_geolocation, check_dim_products]

check_dim_customers >> optimize_dim_customers
optimize_dim_customers >> check_dim_mql

check_dim_geolocation >> optimize_dim_geolocation
optimize_dim_geolocation >> check_dim_mql

check_dim_products >> optimize_dim_products
optimize_dim_products >> check_dim_sellers

check_dim_mql >> optimize_dim_mql

optimize_dim_mql >> [check_fact_order_item, check_fact_orders]

check_dim_orders >> optimize_dim_orders
optimize_dim_orders >> check_dim_product_category

check_dim_product_category >> optimize_dim_product_category

check_dim_sellers >> optimize_dim_sellers
optimize_dim_sellers >> check_fact_order_item

check_dim_products >> optimize_dim_products

optimize_fact_order_item >> [check_fact_orders, check_fact_closed_deals]

check_fact_order_item >> optimize_fact_order_item

optimize_fact_order_item >> optimize_fact_orders
optimize_fact_orders >> check_fact_closed_deals

check_fact_closed_deals >> optimize_fact_closed_deals

optimize_dim_customers >> optimize_dim_geolocation
optimize_dim_customers >> optimize_dim_products
optimize_dim_geolocation >> optimize_dim_mql
optimize_dim_products >> check_dim_sellers
optimize_dim_mql >> [optimize_fact_order_item, optimize_fact_orders]
optimize_fact_order_item >> optimize_fact_orders

optimize_fact_closed_deals >> end_dag
