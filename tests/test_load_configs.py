import unittest
import yaml
from io import StringIO
from data_access.db_credentials import DBCredentials
from data_migration.mapper import Mapper
from data_migration.rule import Rule
from configs.yaml_manager import load_credentials, load_csv_loader
from system_logging.log_manager import instance, Level, log

class TestConfigLoaders(unittest.TestCase):
    def setUp(self):
        self.config_yaml = """
# Database connection configurations
databases_connections:
  database_alias_name:
    host: localhost
    port: 5432
    user: your_username
    database: your_database
    password: your_password
    schema: public
    type: postgresql
  
  another_database:
    host: localhost
    port: 5432
    user: your_username
    database: your_database
    password: your_password
    schema: your_schema
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
        database_alias_name:
          - select * from table_1
          - select ... inner join ..
        another_database:
          - select * from table_2
      outputs:
        another_database:
          - table_3
          - table_4

# CSV loader configurations
csv_loader:
  target_database: another_database
  buffer_size: 10000
  bulk_commit: false
  csv_files:
    - path: path/to/your/csv_file_1.csv
      target_table: table_1
    - path: path/to/your/csv_file_2.csv
      target_table: table_2
    - path: path/to/your/csv_file_3.csv
      target_table: table_3
    - path: path/to/your/csv_file_4.csv
      target_table: table_4
      replace_columns_values:
        column_1: '[private_data]'
        column_2: '[private_data]'
      delimiter: ','
      quotechar: '"'
      encoding: utf-8
        """
        self.configs = yaml.safe_load(StringIO(self.config_yaml))

    def test_load_credentials(self):
        credentials = load_credentials(self.configs)
        self.assertIsInstance(credentials, dict)
        self.assertIn('database_alias_name', credentials)
        self.assertIsInstance(credentials['database_alias_name'], DBCredentials)

    def test_load_csv_loader(self):
        load_csv_return = (load_csv_loader(self.configs))
        target_credentials = load_csv_return['credentials']
        buffer_size = load_csv_return['buffer_size']
        bulk_commit = load_csv_return['bulk_commit']
        csv_files = load_csv_return['csv_files']
        
        self.assertIsInstance(target_credentials, DBCredentials)
        self.assertEqual(buffer_size, 10000)
        self.assertFalse(bulk_commit)
        self.assertIsInstance(csv_files, list)
        self.assertEqual(len(csv_files), 4)

    def test_load_system_logging(self):
        log_manager = instance()  
        self.assertEqual(len(log_manager.observers), 1)
        log(Level.INFO, 'INFO MSG')
        log(Level.DEBUG, 'DEBUG MSG')  # Should not be displayed since DEBUG is commented out
        log(Level.ERROR, 'ERROR MSG')
        log(Level.WARNING, 'WARNING MSG')

    def test_load_data_migration(self):
        mapper = Mapper(configs=self.configs)
        self.assertEqual(mapper.buffer_size, 10000)
        self.assertFalse(mapper.bulk_commit)
        self.assertIsInstance(mapper.rules, list)
        self.assertEqual(len(mapper.rules), 1)
        rule = mapper.rules[0]
        self.assertIsInstance(rule, Rule)
        self.assertEqual(len(rule.inputs), 3)
        self.assertEqual(len(rule.outputs), 2)

if __name__ == '__main__':
    unittest.main()