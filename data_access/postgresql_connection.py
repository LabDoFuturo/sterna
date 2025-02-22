import psycopg2
from system_logging.log_manager import log, Level

class PostgreSQLConnection:
    """
    A class to manage a connection to a PostgreSQL database.

    Attributes:
        db_credentials (DBCredentials): The database credentials.
        connection (psycopg2.extensions.connection): The PostgreSQL connection object.
        database (str): The name of the database.
    """

    def __init__(self, db_credentials):
        self.db_credentials = db_credentials
        self.connection = None
        self.database = db_credentials.database
        
    def create(self):
        try:
            self.connection = psycopg2.connect(
                dbname=self.db_credentials.database,
                user=self.db_credentials.user,
                password=self.db_credentials.password,
                host=self.db_credentials.host,
                port=self.db_credentials.port
            )
            log(Level.DEBUG, f"PostgreSQL connection {self.db_credentials.name} created successfully.")
        except Exception as e:
            log(Level.ERROR, "Error creating PostgreSQL connection")
            raise e

    def close(self):
        if self.connection:
            try:
                self.connection.close()
                log(Level.DEBUG, f"PostgreSQL connection {self.db_credentials.name} closed.")
                self.connection = None
            except Exception as e:
                log(Level.ERROR, "Error closing PostgreSQL connection")
                raise e