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
    'Ecom_Vacuum_DataWarehouse',
    default_args=default_args,
    description='Run vacuum scripts for Delta tables in a tree structure using SSHOperator',
    schedule_interval='@weekly',
    start_date=datetime.now(),
    catchup=False,
)

ssh_hook_spark = SSHHook(ssh_conn_id='spark_server', cmd_timeout=None)


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


vacuum_dir = "/opt/spark-apps/vacuum/warehouse"

# Tạo các tác vụ VACUUM cho từng bảng
vacuum_dim_customers = create_ssh_vacuum_task('vacuum_dim_customers',
                                              f'{vacuum_dir}/vacuum_dim_customers.py')
vacuum_dim_dates = create_ssh_vacuum_task('vacuum_dim_dates',
                                          f'{vacuum_dir}/vacuum_dim_dates.py')
vacuum_dim_geolocation = create_ssh_vacuum_task('vacuum_dim_geolocation',
                                                f'{vacuum_dir}/vacuum_dim_geolocation.py')
vacuum_dim_mql = create_ssh_vacuum_task('vacuum_dim_mql',
                                        f'{vacuum_dir}/vacuum_dim_mql.py')
vacuum_dim_orders = create_ssh_vacuum_task('vacuum_dim_orders',
                                           f'{vacuum_dir}/vacuum_dim_orders.py')
vacuum_dim_product_category = create_ssh_vacuum_task('vacuum_dim_product_category',
                                                     f'{vacuum_dir}/vacuum_dim_productCategory.py')
vacuum_dim_products = create_ssh_vacuum_task('vacuum_dim_products',
                                             f'{vacuum_dir}/vacuum_dim_products.py')
vacuum_dim_sellers = create_ssh_vacuum_task('vacuum_dim_sellers',
                                            f'{vacuum_dir}/vacuum_dim_sellers.py')
vacuum_fact_closed_deals = create_ssh_vacuum_task('vacuum_fact_closed_deals',
                                                  f'{vacuum_dir}/vacuum_fact_closed_deals.py')
vacuum_fact_order_item = create_ssh_vacuum_task('vacuum_fact_order_item',
                                                f'{vacuum_dir}/vacuum_fact_orderItem.py')
vacuum_fact_orders = create_ssh_vacuum_task('vacuum_fact_orders',
                                            f'{vacuum_dir}/vacuum_fact_orders.py')

# Tạo các tác vụ kiểm tra dữ liệu cho từng bảng
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

start_vacuum_dag = DummyOperator(
    task_id='start_dag',
    dag=dag,
)

end_vacuum_dag = DummyOperator(
    task_id='end_dag',
    dag=dag,
)

# Cấu trúc cây cho các tác vụ kiểm tra và vacuum
check_group_1 = [
    check_dim_customers,
    check_dim_dates,
    check_dim_geolocation
]

check_group_2 = [
    check_dim_mql,
    check_dim_orders,
    check_dim_product_category
]

check_group_3 = [
    check_dim_products,
    check_dim_sellers
]

check_fact_group = [
    check_fact_closed_deals,
    check_fact_order_item,
    check_fact_orders
]

vacuum_group_1 = [
    vacuum_dim_customers,
    vacuum_dim_dates,
    vacuum_dim_geolocation
]

vacuum_group_2 = [
    vacuum_dim_mql,
    vacuum_dim_orders,
    vacuum_dim_product_category
]

vacuum_group_3 = [
    vacuum_dim_products,
    vacuum_dim_sellers
]

vacuum_fact_group = [
    vacuum_fact_closed_deals,
    vacuum_fact_order_item,
    vacuum_fact_orders
]

# Thiết lập các phụ thuộc cho từng tác vụ kiểm tra
for check_task in check_group_1:
    start_vacuum_dag >> check_task

for check_task in check_group_2:
    start_vacuum_dag >> check_task

for check_task in check_group_3:
    start_vacuum_dag >> check_task

for check_task in check_fact_group:
    start_vacuum_dag >> check_task

for check_task in check_fact_group:
    for vacuum_task in vacuum_fact_group:
        check_task >> vacuum_task
# Thiết lập phụ thuộc cho các tác vụ vacuum
for vacuum_task in vacuum_group_1:
    for check_task in check_group_1:
        check_task >> vacuum_task

for vacuum_task in vacuum_group_2:
    for check_task in check_group_2:
        check_task >> vacuum_task

for vacuum_task in vacuum_group_3:
    for check_task in check_group_3:
        check_task >> vacuum_task

# Thiết lập phụ thuộc cho các tác vụ vacuum fact
for vacuum_fact_task in vacuum_fact_group:
    for vacuum_task in vacuum_group_1 + vacuum_group_2 + vacuum_group_3:
        vacuum_task >> vacuum_fact_task

# Kết thúc DAG
for vacuum_fact_task in vacuum_fact_group:
    vacuum_fact_task >> end_vacuum_dag
