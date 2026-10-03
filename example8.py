from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator

# Assume we are already inside a 'with DAG(...) as dag:' block

start_task = DummyOperator(task_id='start')

# 1. Open the TaskGroup context manager
with TaskGroup(group_id='my_processing_group') as processing_group:
    
    # 2. Any tasks defined indented here belong to the group
    task_1 = DummyOperator(task_id='transform_data')
    task_2 = DummyOperator(task_id='validate_data')
    
    # You can define internal dependencies just for the tasks in the group
    task_1 >> task_2

end_task = DummyOperator(task_id='end')

# 3. You can set dependencies using the entire group at once!
start_task >> processing_group >> end_task