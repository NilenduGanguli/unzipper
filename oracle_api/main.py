from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
import json

app = FastAPI()

class OracleDBConfig(BaseModel):
    host: str
    port: str
    user: str
    password: str
    sid: str

class QueryModel(BaseModel):
    query: str

class InsertRecordsModel(BaseModel):
    table: str
    records: list[dict]

class UpdateRecordModel(BaseModel):
    table: str
    primary_key: str
    updates: dict

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
            return rows
        except cx_Oracle.DatabaseError as e:
            raise HTTPException(status_code=400, detail=str(e))
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
            return {"status": "success"}
        except cx_Oracle.DatabaseError as e:
            raise HTTPException(status_code=400, detail=str(e))
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
            return {"status": "success"}
        except cx_Oracle.DatabaseError as e:
            raise HTTPException(status_code=400, detail=str(e))
        finally:
            self.cursor.close()
            self.connection.close()

def get_db(config: OracleDBConfig):
    return OracleDBUtility(config.host, config.port, config.user, config.password, config.sid)

@app.post("/execute-query")
def execute_query(config: OracleDBConfig, query_model: QueryModel):
    db = get_db(config)
    result = db.execute_query(query_model.query)
    return {"result": result}

@app.post("/insert-records")
def insert_records(config: OracleDBConfig, insert_model: InsertRecordsModel):
    db = get_db(config)
    result = db.insert_multiple_records(insert_model.table, insert_model.records)
    return result

@app.put("/update-record")
def update_record(config: OracleDBConfig, update_model: UpdateRecordModel):
    db = get_db(config)
    result = db.update_record(update_model.table, update_model.primary_key, update_model.updates)
    return result
