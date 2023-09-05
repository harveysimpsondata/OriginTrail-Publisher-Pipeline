from duckdb import connect

class SQL:
    def __init__(self, sql, **bindings):
        self.sql = sql
        self.bindings = bindings

class DuckDB:
    def __init__(self, options=""):
        self.options = options

    def query(self, select_statement: SQL):
        db = connect(":memory:")
        db.query("install httpfs; load httpfs;")
        db.query(self.options)

