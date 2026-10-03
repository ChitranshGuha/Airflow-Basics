from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# 1. Default Arguments
default_args = {
    'owner': 'data_engineering',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 2. Python callable for a task
def process_stock_data():
    print("Processing daily stock metrics...")
    # Logic to process data would go here

# 3. DAG Instantiation using the context manager ('with' statement)
with DAG(
    dag_id='stock_market_daily_extract',
    default_args=default_args,
    description='A simple daily stock data pipeline',
    start_date=datetime(2026, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    # 4. Tasks definition
    extract_task = BashOperator(
        task_id='extract_raw_data',
        bash_command='echo "Extracting raw stock data from API"'
    )

    process_task = PythonOperator(
        task_id='process_data',
        python_callable=process_stock_data
    )

    # 5. Setting Dependencies
    extract_task >> process_task