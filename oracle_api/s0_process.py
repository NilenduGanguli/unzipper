import os
import requests
import json

def read_sql_file(file_path):
    with open(file_path, 'r') as file:
        return file.read()

def execute_query(api_url, query):
    response = requests.post(f"{api_url}/execute-query", json={"query": query})
    if response.status_code != 200:
        raise Exception(f"Failed to execute query: {response.text}")
    return response.json()

def sync_logic(data):
    # Implement your sync logic here
    # For now, we will assume it just passes the data through
    return data

def insert_records(api_url, table, records):
    response = requests.post(f"{api_url}/insert-records", json={"table": table, "records": records})
    if response.status_code != 200:
        raise Exception(f"Failed to insert records: {response.text}")
    return response.json()

def main():
    # Load environment variables
    api_url = os.getenv('API_URL')
    sql_file_path = os.getenv('SQL_FILE_PATH')
    target_table = os.getenv('TARGET_TABLE')
    
    # Read SQL query from file
    query = read_sql_file(sql_file_path)
    
    # Execute query and get the result
    query_result = execute_query(api_url, query)
    
    # Sync logic to prepare data for insertion
    records = sync_logic(query_result["result"])
    
    # Insert records into the database
    insert_response = insert_records(api_url, target_table, records)
    
    print(f"Insert response: {insert_response}")

if __name__ == "__main__":
    main()
