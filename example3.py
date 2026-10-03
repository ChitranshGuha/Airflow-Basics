from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator # Often used as a placeholder

def choose_path():
    day_of_week = "Saturday"
    if day_of_week == "Saturday" or day_of_week == "Sunday":
        return 'weekend_skip_task' # Must exactly match a downstream task_id
    else:
        return 'weekday_process_task'

branch_decision = BranchPythonOperator(
    task_id='decide_workflow',
    python_callable=choose_path
)

process = DummyOperator(task_id='weekday_process_task')
skip = DummyOperator(task_id='weekend_skip_task')

# You can pass a list of tasks to branch into multiple paths
branch_decision >> [process, skip]