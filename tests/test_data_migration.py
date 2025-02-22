import unittest
import yaml
from io import StringIO
from data_access.db_credentials import DBCredentials
from data_migration.mapper import Mapper
from data_migration.rule import Rule
from configs.yaml_manager import load_credentials, load_csv_loader
from system_logging.log_manager import LogManager
from csv_loader.csv_to_database import csv_importer  

class TestDataMigration(unittest.TestCase):
    def setUp(self):
        self.config_yaml = """
# Database connection configurations
databases_connections:
  database_1:
    host: localhost
    port: 5432
    user: your_username
    database: your_database
    password: your_password
    schema: public
    type: postgresql
    
  database_2:
    host: localhost
    port: 5432
    user: your_username
    database: your_database
    password: your_password
    schema: public
    type: postgresql
 
# System logging configurations
system_logging:
  console_log:
    levels:
      - INFO
      - ERROR
      - WARNING
      - DEBUG
      
# Data migration configurations
data_migration:
  buffer_size: 10000
  bulk_commit: false
  rules:
    rule_1:
      inputs:
        database_1:
          - select * from table_1
      outputs:
        database_2:
          - table_2
  """
        self.configs = yaml.safe_load(StringIO(self.config_yaml))

    def test_csv(self):
        credentials = load_credentials(self.configs)
        Mapper(configs=self.configs).start_migration()

if __name__ == '__main__':
    unittest.main()