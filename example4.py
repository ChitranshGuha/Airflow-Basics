from airflow.sensors.python import PythonSensor

def is_api_ready():
    # In reality, this would make an API call to check status
    # Returning False means "keep waiting". Returning True means "proceed".
    return True 

wait_for_api = PythonSensor(
    task_id='wait_for_api_task',
    python_callable=is_api_ready,
    poke_interval=30,  # Check every 30 seconds
    timeout=600,       # Fail the task if it hasn't succeeded after 10 minutes
    mode='poke'        # 'poke' keeps the worker slot occupied. 'reschedule' frees it up between checks.
)