from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.sensors.python import PythonSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.utils.task_group import TaskGroup

def send_slack_alert(context):
    task_id = context.get('task_instance').task_id
    dag_id = context.get('task_instance').dag_id
    log_url = context.get('task_instance').log_url
    
    message = f" Task *{task_id}* failed in DAG *{dag_id}*!\nCheck logs here: {log_url}"
    
    slack_hook = SlackWebhookHook(slack_webhook_conn_id='slack_alerts_connection')
    slack_hook.send_text(text=message)
    
def sync_data():
    p = PostgresHook(postgres_conn_id = 'warehouse_db')
    s = "SELECT MAX(sync_id) FROM sync_history;"
    r = p.get_records(sql=s)
    m = r[0][0]
    print("Data synced successfully.")
    return m

def audit_log(ti):
    rc = ti.xcom_pull(task_ids='A.sync_data')
    print(f"I received {rc} from the previous task!")
    
def check_day():
    return 'A.sync_data'

def check_system_ready():
    return True

def sla_miss_alert(dag, task_list, blocking_task_list, slas, blocking_tis):
    print("SLA alert tells you if a task is taking too long to finish")
    print("Unlike on_failure_callback which is triggered immediately by the worker running the task, SLAs are checked periodically by the main Airflow Scheduler. This means SLA alerts might arrive a few minutes after the actual deadline passes.")
    
def_args = {
    'owner' : 'platform_team',
    'on_failure_callback': send_slack_alert,
    'retries' : 2,
    'retry_delay' : timedelta(minutes = 5),
    'sla': timedelta(hours=2)
}

with DAG(
    dag_id = 'warehouse_sync_pipeline',
    default_args = def_args,
    sla_miss_callback=sla_miss_alert,
    description = 'Task1',
    start_date=datetime(2026,10,2),
    schedule_interval='@hourly',
    catchup=False
) as dag:
    
    taskA = BashOperator(
        task_id = 'check_connection',
        bash_command = 'echo "Checking Snowflake connection for date:"{{ds}}'
    )    
    
    with TaskGroup(group_id='A') as p_group:
        taskB = PythonOperator(
            task_id = 'sync_data',
            python_callable = sync_data
        )
        
        taskC = PythonOperator(
            task_id = 'audit_log',
            python_callable = audit_log
        )
        taskB >> taskC
        
    branch_task = BranchPythonOperator(
        task_id = 'check_if_weekday',
        python_callable = check_day
    )
    skip_task = BashOperator(
        task_id = 'skip_sync',
        bash_command = 'echo "Skipping weekend sync"'
    )
    sensor_task = PythonSensor(
        task_id = 'wait_for_system',
        python_callable = check_system_ready,
        poke_interval = 10,
        timeout = 60,
        mode = 'poke'
    )
    sensor_task >> taskA
    taskA >> branch_task >> [p_group,skip_task] 