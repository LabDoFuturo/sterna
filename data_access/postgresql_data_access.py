import psycopg2
import psycopg2.extras
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import adapt

from system_logging.log_manager import log, Level
from data_access.utils import format_reserved_word


def postgres_execute_DDL(postgresql, sql):
    """
    Executes a DDL (Data Definition Language) SQL statement on a PostgreSQL database.
    
    """
    try:
        connection = postgresql.connection
        cursor = connection.cursor()
        log(Level.SQL, f"Query: {sql}")
        cursor.execute(sql)
    except Exception as e:
        log(Level.ERROR, f"Error executing PostgreSQL DDL query")
        raise e
    finally:
        if cursor:
            cursor.close()
            
def postgres_all_tables_names(postgresql):
    """
    Retrieves the names of all tables in the PostgreSQL database.
    
    """
    try:
        connection = postgresql.connection
        cursor = connection.cursor()
        schema_name = postgresql.db_credentials.schema
        sql = f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{schema_name}'
            AND table_type = 'BASE TABLE'
        """
        log(Level.SQL, f"Query: {sql}")
        cursor.execute(sql)
        table_names = [row[0] for row in cursor.fetchall()]
        return table_names
    except Exception as e:
        log(Level.ERROR, f"Error getting table names from PostgreSQL")
        raise e
    finally:
        if cursor:
            cursor.close()

def postgres_commit(postgresql):
    try:
        postgresql.connection.commit()
    except Exception as e:
        log(Level.ERROR, f"Error committing data to PostgreSQL")
        raise e

class PostgreSQLWriter:
    """
    A class to handle writing data to a PostgreSQL table.

    Attributes:
        bulk_commit (bool): Whether to commit transactions in bulk.
        postgresql (PostgreSQLConnection): The PostgreSQL connection object.
        table (Table): The table metadata object.
        buffer_size (int): The size of the buffer for bulk inserts.
        buffer (list): The buffer to store rows before inserting.
        schema (str): The schema of the table.
        column_names (str): The comma-separated column names for the insert statement.
        insert_sql (str): The SQL insert statement template.
        cursor (psycopg2.cursor): The cursor for executing SQL statements.
        template (str): The template for inserting values.
    """

    def __init__(self, postgresql, table, schema="", buffer_size=100, bulk_commit=True):
        self.bulk_commit = bulk_commit
        self.postgresql = postgresql
        self.table = table
        self.buffer_size = buffer_size
        self.buffer = []
        if schema != "":
            schema += "."
        self.schema = schema
        self.cursor = None
        if table.columns:
            self.set_columns([(column.name) for column in self.table.columns])
        
        
    def set_columns(self, columns):
        self.column_names = ', '.join([format_reserved_word(column) for column in columns])
        self.insert_sql = f"INSERT INTO {self.schema}{self.table.name} ({self.column_names}) VALUES %s"
        parts = []
        for column in columns:
            parts.append("%s")
        self.template = f"({', '.join(parts)})"

    def insert(self, data):
        """
        Inserts a row of data into the buffer. If the buffer size is reached, flushes the buffer.

        """
        if isinstance(data, dict):
            data = [data[column.name] for column in self.table.columns]
        self.buffer.append(data)
        if len(self.buffer) >= self.buffer_size:
            return self.flush_buffer()
        return False

    def flush_buffer(self):
        """
        Flushes the buffer by inserting all buffered rows into the PostgreSQL table.
        
        """
        if not self.buffer or len(self.buffer) == 0:
            return False
        try:
            self.cursor = self.postgresql.connection.cursor()
            psycopg2.extras.execute_values(self.cursor, self.insert_sql, self.buffer, template=self.template)
            num_inserted_rows = len(self.buffer)
            log(Level.DEBUG, f"Inserted {num_inserted_rows} rows into {self.schema}{self.table.name}.")
            #log(Level.SQL, f"Query: {self.format_sql_log(self.insert_sql, self.buffer)}")
            log(Level.SQL, f"Query: {self.insert_sql} {self.buffer}")
            self.buffer.clear()
        except Exception as e:
            log(Level.DEBUG, f"Error inserting data into PostgreSQL, table: {self.table.name}")
            raise e
        finally:
            self.close_cursor()
        if self.bulk_commit:
            self.commit()
            return True

    def close_cursor(self):
        if self.cursor and not self.cursor.closed:
            self.cursor.close()
            self.cursor = None

    def commit(self):
        try:
            self.postgresql.connection.commit()
        except Exception as e:
            log(Level.ERROR, f"Error committing data to PostgreSQL")
            raise e
        self.close_cursor()

    def rollback(self):
        try:
            self.postgresql.connection.rollback()
        except Exception as e:
            log(Level.ERROR, f"Error rollback data to PostgreSQL")
            raise e
        self.close_cursor()

    #def format_sql_log(self, sql, values):
    #    def adapt_row(row):
    #        return "(" + ", ".join(adapt(v).getquoted().decode('utf-8', errors='backslashreplace') if v is not None else 'NULL' for v in row) + ")"
    #    return f"{sql} VALUES {', '.join(adapt_row(row) for row in values)}"
        
        
class PostgresTableIterator:
    """
    A class to handle reading data from a PostgreSQL table.      
    """
    def __init__(self, postgresql, table_name=None, schema=None, query=None, batch_size=1000):     
        self.postgresql = postgresql
        self.table_name = table_name
        self.batch_size = batch_size
        self.query = query
        self.schema = schema
        self.cursor = None
        self.current_batch = []

    def __iter__(self):
        return self

    def __next__(self):
        if self.cursor is None:
            self.cursor = self.postgresql.connection.cursor(cursor_factory=RealDictCursor)
            sql = None
            if self.query:
                sql = self.query
            elif self.schema:
                sql = f"SELECT * FROM {self.schema}.{self.table_name}"
            else:
                sql = f"SELECT * FROM {self.table_name}"
            
            if sql is None:
                raise Exception("No query or table name provided.")
            log(Level.SQL, f"Query: {sql}")
            self.cursor.execute(sql)

        if not self.current_batch:
            self.current_batch = self.cursor.fetchmany(self.batch_size)
            if not self.current_batch:
                self.cursor.close()
                raise StopIteration

        row = self.current_batch.pop(0)
        return row

    def close(self):
        if self.cursor:
            self.cursor.close()
            self.cursor = None
      