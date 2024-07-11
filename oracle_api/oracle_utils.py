import cx_Oracle
import json

class OracleDBUtility:
    def __init__(self, host, port, user, password, sid):
        self.dsn = cx_Oracle.makedsn(host, port, sid=sid)
        self.connection = cx_Oracle.connect(user, password, self.dsn)
        self.cursor = self.connection.cursor()

    def execute_query(self, query):
        try:
            self.cursor.execute(query)
            columns = [col[0] for col in self.cursor.description]
            rows = [dict(zip(columns, row)) for row in self.cursor.fetchall()]
            return json.dumps(rows)
        except cx_Oracle.DatabaseError as e:
            return json.dumps({"error": str(e)})
        finally:
            self.cursor.close()
            self.connection.close()

    def insert_multiple_records(self, table, records):
        try:
            self.cursor = self.connection.cursor()
            columns = records[0].keys()
            query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join([':'+str(i+1) for i in range(len(columns))])})"
            data = [tuple(record.values()) for record in records]
            self.cursor.executemany(query, data)
            self.connection.commit()
            return json.dumps({"status": "success"})
        except cx_Oracle.DatabaseError as e:
            return json.dumps({"error": str(e)})
        finally:
            self.cursor.close()
            self.connection.close()

    def update_record(self, table, primary_key, updates):
        try:
            self.cursor = self.connection.cursor()
            set_clause = ", ".join([f"{col} = :{col}" for col in updates.keys()])
            query = f"UPDATE {table} SET {set_clause} WHERE {primary_key} = :primary_key_value"
            updates["primary_key_value"] = updates.pop(primary_key)
            self.cursor.execute(query, updates)
            self.connection.commit()
            return json.dumps({"status": "success"})
        except cx_Oracle.DatabaseError as e:
            return json.dumps({"error": str(e)})
        finally:
            self.cursor.close()
            self.connection.close()
