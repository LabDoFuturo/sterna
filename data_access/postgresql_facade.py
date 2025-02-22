from data_access.postgresql_connection import PostgreSQLConnection
from data_access.postgresql_data_access import PostgreSQLWriter, PostgresTableIterator, postgres_execute_DDL, postgres_commit
from data_access.postgresql_metadata_access import PostgreSQLTableManager
from system_logging.log_manager import log, Level
from data_access.utils import unique_timestamp_string_id

from data_access.postgresql_connection import PostgreSQLConnection

class PostgreSQLFacade:
    connections_pool = {}  # Static shared connection pool
    
    def __init__(self, db_credentials, table=None, table_name=None, buffer_size=1000, bulk_commit=False, query=None, use_columns_metadata=True):
        self.db_credentials = db_credentials
        self.reuse = False
        self.connection = None
        self.table = table
        self.table_name = table_name
        self.buffer_size = buffer_size
        self.bulk_commit = bulk_commit
        self.query = query
        self.use_columns_metadata = use_columns_metadata

    def create_connection(self, reuse=False):
        self.reuse = reuse
        if self.reuse:
            if self.db_credentials.name in PostgreSQLFacade.connections_pool:
                self.connection = PostgreSQLFacade.connections_pool[self.db_credentials.name]
            else:
                self.connection = PostgreSQLConnection(self.db_credentials)
                self.connection.create()
                PostgreSQLFacade.connections_pool[self.db_credentials.name] = self.connection
        else:
            self.connection = PostgreSQLConnection(self.db_credentials)
            self.connection.create()
            new_name = unique_timestamp_string_id()
            PostgreSQLFacade.connections_pool[new_name] = self.connection
    
    def close_connection(self):
        if not self.reuse and self.connection:
            self.connection.close()
    
    def close_all_connections():
        for conn in PostgreSQLFacade.connections_pool.values():
            conn.close()
            
    def writer(self, table=None, buffer_size=None, bulk_commit=None):
        if not table:
            table = self.table
        if not buffer_size:
            buffer_size = self.buffer_size
        if not bulk_commit:
            bulk_commit = self.bulk_commit
            
        if not self.connection:
            raise Exception('Connection not created')
        
        if self.use_columns_metadata:
            meta = self.metadata(table_name=table.name)
            table.columns = meta.get_table_columns()
        
        postgres_writer = PostgreSQLWriter(
            self.connection, 
            table, 
            schema=self.db_credentials.schema, 
            buffer_size=buffer_size, 
            bulk_commit=bulk_commit)
        return postgres_writer
    
    def reader(self, table=None, query=None, batch_size=1000):
        if not table:
            table = self.table
        if not query:
            query = self.query
            
        if not self.connection:
            raise Exception('Connection not created')
        postgres_reader = PostgresTableIterator(
            self.connection, 
            table, 
            schema=self.db_credentials.schema, 
            query=query)
        return postgres_reader
    
    def metadata(self, table_name=None):
        if not table_name:
            table_name = self.table_name
        
        if not self.connection:
            raise Exception('Connection not created')
        postgres_meta = PostgreSQLTableManager(
            self.connection, 
            table_name, 
            schema=self.db_credentials.schema)
        return postgres_meta
    
    def execute_DDL(self, sql):
        if not self.connection:
            raise Exception('Connection not created')
        postgres_execute_DDL(self.connection, sql)
        
    def commit(self):
        if not self.connection:
            raise Exception('Connection not created')
        postgres_commit(self.connection)