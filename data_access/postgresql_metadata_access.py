from system_logging.log_manager import log, Level
from data_access.metadata_models import Column


class PostgreSQLTableManager:
    """
    A class to manage PostgreSQL table metadata.

    Attributes:
        postgresql (PostgreSQLConnection): The PostgreSQL connection object.
        table_name (str): The name of the table.
        schema (str): The schema of the table.
        cursor (psycopg2.cursor): The cursor for executing SQL statements.
    """
    def __init__(self, postgresql, table_name, schema=""):
        self.postgresql = postgresql
        self.table_name = table_name
        if schema != "":
            schema += "."
        self.schema = schema
        self.cursor = None
        
        
    def get_table_columns(self):
        """
        Retrieves the columns of the table from the PostgreSQL database.
        
        """
        try:
            self.cursor = self.postgresql.connection.cursor()
            
            schema_name = self.schema[:-1] if self.schema else "public"
            
            sql = f"""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema = '{schema_name}' AND table_name = '{self.table_name}'
            """
            log(Level.DEBUG, f"Query: {sql}")
            self.cursor.execute(sql)
            columns = [
                Column(
                    name=row[0],
                    data_type=row[1],
                    nullable=row[2] == "YES",
                    default=row[3]
                )
                for row in self.cursor.fetchall()
            ]
            return columns
        except Exception as e:
            log(Level.ERROR, f"Error getting columns from PostgreSQL table")
            raise e
        finally:
            self.close_cursor()

    def get_table_row_count(self):
        """
        Retrieves the row count of the table from the PostgreSQL database.
        
        """
        try:
            self.cursor = self.postgresql.connection.cursor()
            sql = f"SELECT COUNT(*) FROM {self.schema}{self.table_name}"
            self.cursor.execute(sql)
            row_count = self.cursor.fetchone()[0]
            return row_count if row_count is not None else 0
        except Exception as e:
            log(Level.ERROR, f"Error getting row count from PostgreSQL table")
            raise e
        finally:
            self.close_cursor()
            
    def get_max_id(self, id_column):
        """
        Retrieves the maximum value of the specified ID column from the table.

        """
        try:
            self.cursor = self.postgresql.connection.cursor()
            sql = f"SELECT MAX({id_column}) FROM {self.schema}{self.table_name}"
            log(Level.DEBUG, f"Query: {sql}")
            self.cursor.execute(sql)
            max_id = self.cursor.fetchone()[0]
            return max_id
        except Exception as e:
            log(Level.ERROR, f"Error getting maximum value of ID column from PostgreSQL table")
            raise e
        finally:
            self.close_cursor()

    def truncate_table(self):
        """
        Truncates the table, removing all rows (WARNING: is using CASCADE).

        """
        try:
            self.cursor = self.postgresql.connection.cursor()
            sql = f"TRUNCATE TABLE {self.schema}{self.table_name} CASCADE"
            log(Level.DEBUG, f"Query: {sql}")
            self.cursor.execute(sql)
            self.postgresql.connection.commit()
            log(Level.DEBUG, f"Table {self.schema}{self.table_name} truncated.")
        except Exception as e:
            log(Level.ERROR, f"Error truncating PostgreSQL table:")
            raise e
        finally:
            self.close_cursor()

    def reset_sequence(self):
        """
        Resets the sequence associated with the table's ID column to start with 1.

        """
        try:
            self.cursor = self.postgresql.connection.cursor()
            sql = f"ALTER SEQUENCE {self.schema}{self.table_name}_id_seq RESTART WITH 1"
            log(Level.DEBUG, f"Query: {sql}")
            self.cursor.execute(sql)
            self.postgresql.connection.commit()
            log(Level.DEBUG, f"Sequence for table {self.schema}{self.table_name} reset.")
        except Exception as e:
            log(Level.ERROR, f"Error resetting sequence for PostgreSQL table:")
            raise e
        finally:
            self.close_cursor()

    def get_sequence_current_value(self):
        """
        Retrieves the current value of the sequence associated with the table's ID column.

        """
        try:
            self.cursor = self.postgresql.connection.cursor()
            sql = f"SELECT last_value FROM {self.schema}{self.table_name}_id_seq"
            log(Level.DEBUG, f"Query: {sql}")
            self.cursor.execute(sql)
            current_value = self.cursor.fetchone()[0]
            return current_value
        except Exception as e:
            log(Level.ERROR, f"Error getting current value from PostgreSQL sequence")
            raise e
        finally:
            self.close_cursor()
    
    def set_sequence_value(self, value):
        """
        Sets the value of the sequence associated with the table's ID column.

        """
        try:
            self.cursor = self.postgresql.connection.cursor()
            sql = f"SELECT setval('{self.schema}{self.table_name}_id_seq', %s, false)"
            log(Level.DEBUG, f"Query: {sql}")
            self.cursor.execute(sql, (value,))
            self.postgresql.connection.commit()
            log(Level.DEBUG, f"Sequence value for table {self.schema}{self.table_name} set to {value}.")
        except Exception as e:
            log(Level.ERROR, f"Error setting sequence value for PostgreSQL table")
            raise e
        finally:
            self.close_cursor()

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
            log(Level.ERROR, f"Error committing data to PostgreSQL")
            raise e  
        self.close_cursor()