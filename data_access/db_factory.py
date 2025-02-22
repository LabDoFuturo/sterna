from data_access.postgresql_facade import PostgreSQLFacade

class DatabaseFactory:
    _instance = None
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabaseFactory, cls).__new__(cls)
        return cls._instance
    
    def create(self, db_credentials, table=None, table_name=None, buffer_size=1000, bulk_commit=False, query=None):
        if db_credentials.type == "postgresql":
            return PostgreSQLFacade(db_credentials, table=table, table_name=table_name, buffer_size=buffer_size, bulk_commit=bulk_commit, query=query)
        else:
            raise Exception("Unsupported database type")
        
    def close_all_connections(self):
        PostgreSQLFacade.close_all_connections()
        