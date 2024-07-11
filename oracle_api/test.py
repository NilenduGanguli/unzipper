from oracle_utils import OracleDBUtility
# Replace with your Oracle DB credentials and DSN
user = "your_username"
password = "your_password"
dsn = "your_dsn"

db_util = OracleDBUtility(user, password, dsn)

# Example: Execute Query
query_result = db_util.execute_query("SELECT * FROM your_table")
print(query_result)

# Example: Insert Multiple Records
records = [
    {"column1": "value1", "column2": "value2"},
    {"column1": "value3", "column2": "value4"}
]
insert_result = db_util.insert_multiple_records("your_table", records)
print(insert_result)

# Example: Update Record
updates = {"column1": "new_value", "primary_key_column": "primary_key_value"}
update_result = db_util.update_record("your_table", "primary_key_column", updates)
print(update_result)

# export ORACLE_USER="your_username"
# export ORACLE_PASSWORD="your_password"
# export ORACLE_HOSTNAME="your_hostname"
# export ORACLE_PORT="your_port"
# export ORACLE_SID="your_sid"

import os


# Retrieve environment variables
user = os.getenv('ORACLE_USER')
password = os.getenv('ORACLE_PASSWORD')
hostname = os.getenv('ORACLE_HOSTNAME')
port = os.getenv('ORACLE_PORT')
sid = os.getenv('ORACLE_SID')
