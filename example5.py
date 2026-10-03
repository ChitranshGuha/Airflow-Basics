archive_file = BashOperator(
    task_id='archive_yesterdays_data',
    # At runtime, Airflow will replace {{ ds }} with the actual logical date of the run!
    bash_command='mv /data/raw.csv /data/archive_{{ ds }}.csv'
)