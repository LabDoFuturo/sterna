import unittest
import yaml
from io import StringIO
from data_access.db_credentials import DBCredentials
from data_migration.mapper import Mapper
from data_migration.rule import Rule
from configs.yaml_manager import load_credentials, load_csv_loader
from system_logging.log_manager import LogManager
from csv_loader.csv_to_database import csv_importer  

class TestCSVLoader(unittest.TestCase):
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
 
# System logging configurations
system_logging:
  console_log:
    levels:
      - INFO
      - ERROR
      - WARNING
      - DEBUG
      
# CSV loader configurations
csv_loader:
  target_database: database_1
  buffer_size: 10000
  bulk_commit: false
  csv_files:
    - path: path/to/your/csv_file_1.csv
      target_table: table_1
  """
        self.configs = yaml.safe_load(StringIO(self.config_yaml))

    def test_csv(self):
        credential = load_credentials(self.configs)["database_1"]
        params = load_csv_loader(self.configs)

if __name__ == '__main__':
    unittest.main()