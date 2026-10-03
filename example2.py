def extract_data():
    # Returning a value automatically pushes it to XCom
    return "1500_rows"

def summarize_data(ti):
    # 'ti' stands for Task Instance. We use it to pull the data.
    # We specify which task's return value we want using task_ids
    row_count = ti.xcom_pull(task_ids='extract_step')
    print(f"I received {row_count} from the previous task!")

# ... (DAG definition omitted for brevity) ...

t1 = PythonOperator(
    task_id='extract_step',
    python_callable=extract_data
)

t2 = PythonOperator(
    task_id='summarize_step',
    python_callable=summarize_data
)

t1 >> t2