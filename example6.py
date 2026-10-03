from airflow.providers.postgres.hooks.postgres import PostgresHook

def check_user_count():
    # 1. Instantiate the hook with the secure Connection ID
    pg_hook = PostgresHook(postgres_conn_id='my_prod_postgres')
    
    # 2. Use the hook to execute SQL. 
    # get_records() handles opening the cursor, running the query, and safely closing the connection.
    sql_query = "SELECT COUNT(*) FROM active_users;"
    records = pg_hook.get_records(sql=sql_query)
    
    # records is returned as a list of tuples: e.g., [(150,)]
    user_count = records[0][0]
    
    print(f"Found {user_count} active users.")
    return user_count