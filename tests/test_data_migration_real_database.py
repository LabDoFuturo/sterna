import unittest
import yaml
from io import StringIO
from data_migration.mapper import Mapper
import configs.yaml_manager as configs
from data_access.postgresql_data_access import postgres_execute_DDL, postgres_commit
from data_access.postgresql_connection import PostgreSQLConnection
import os
import shutil

class TestDataMigration(unittest.TestCase):
    def setUp(self): 
        self.create_tables_and_data = """

DROP TABLE IF EXISTS table_1;
DROP TABLE IF EXISTS table_2;        

CREATE TABLE table_1 (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    age INT
);

CREATE TABLE table_2 (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    age INT
);

INSERT INTO table_1 (name, age) VALUES ('John', 25);
INSERT INTO table_1 (name, age) VALUES ('Jane', 30);
INSERT INTO table_1 (name, age) VALUES ('Jack', 35);
INSERT INTO table_1 (name, age) VALUES ('Jill', 40);

""" 
        self.rules_file = f"""
def exec(inputs, outputs):
    # zip inputs and outputs
    for input, output in zip(inputs, outputs):
        input.create_connection(reuse=True)
        output.create_connection(reuse=True)
        
        writer = output.writer()
        for row in input.reader():          
            writer.insert(row)
    
        writer.flush_buffer() 
        writer.commit()
"""
        self.config_yaml = """
# Database connection configurations
databases_connections:
  database_1:
    host: localhost
    port: 5432
    user: postgres
    database: postgres
    password: postgres
    schema: public
    type: postgresql
    
  database_2:
    host: localhost
    port: 5432
    user: postgres
    database: postgres
    password: postgres
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

    def test_migration(self):

        # test conecting to database
        db_credentials = configs.load_credentials(self.configs)["database_1"]
        connection = PostgreSQLConnection(db_credentials)
        try:
          connection.create()
        except Exception as e:
          self.skipTest("Error creating connection")
          return
        postgres_execute_DDL(connection, self.create_tables_and_data)
        postgres_commit(connection)
        connection.close()
        
        OLD_PRIVATE_FOLDER = configs.PRIVATE_FOLDER
        private_folder = "tests/private"
        
        configs.update_private_folder(private_folder)
      
        rules_folder = os.path.join(private_folder, "rules")
        rule_file_path = os.path.join(rules_folder, "rule_1.py")
        configs_file_path = os.path.join(private_folder, "configs.yml")
        
        os.makedirs(rules_folder, exist_ok=True)
        
        with open(rule_file_path, 'w') as f:
            f.write(self.rules_file)
        with open(configs_file_path, 'w') as f:
            f.write(self.config_yaml)
        
        
        Mapper().start_migration()
      
        if os.path.exists(private_folder):
          shutil.rmtree(private_folder)
          print(f"Folder '{private_folder}' removed.")
        else:
          print(f"Folder '{private_folder}' does not exist.")
          
        
        connection = PostgreSQLConnection(db_credentials)
        connection.create()
        postgres_execute_DDL(connection, "DROP TABLE IF EXISTS table_1; DROP TABLE IF EXISTS table_2; ")
        postgres_commit(connection)
        connection.close()
        
        configs.update_private_folder(OLD_PRIVATE_FOLDER)

if __name__ == '__main__':
    unittest.main()