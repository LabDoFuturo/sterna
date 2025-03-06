import os
import time
import importlib.util
from configs.yaml_manager import load_data_migration
from system_logging.log_manager import log, Level
from data_access.db_factory import DatabaseFactory
from data_access.metadata_models import Table
class Mapper:
    """
    A singleton class to manage mapping processes.
    """
    _instance = None

    def __new__(cls, configs=None):
        """
        Creates a new instance of Mapper if one does not already exist.

        Returns:
            Mapper: The singleton instance of Mapper.
        """
        if cls._instance is None:
            cls._instance = super(Mapper, cls).__new__(cls)
            cls._instance.rules = []
            cls._instance.buffer_size = 1000
            cls._instance.bulk_commit = False
            load_data_migration(cls._instance, configs=configs)   
            log(Level.DEBUG, f'[data_migration] Starting mapping process (buffer_size: {cls._instance.buffer_size}, bulk_commit: {cls._instance.bulk_commit})\n')
            for rule in cls._instance.rules:
                log(Level.DEBUG, rule)      
        return cls._instance

    def start_migration(self):
        log(Level.INFO, '[data_migration] Starting data migration\n')

        for rule in self.rules:
            start_time = time.perf_counter()

            # Check if the corresponding rule file exists
            rule_file = f"private/rules/{rule.name}.py"
            
            if not os.path.isfile(rule_file):
                raise FileNotFoundError(f"Rule file {rule_file} not found")

            # Load the module dynamically
            spec = importlib.util.spec_from_file_location(rule.name, rule_file)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            # Ensure the module contains the 'exec' function
            if not hasattr(module, "exec"):
                raise AttributeError(f"Function 'exec' not found in {rule_file}")

            # Execute the rule function with inputs and outputs
            
            database_inputs = []
            for input in rule.inputs:
                database_inputs.append(DatabaseFactory().create(
                    input.credentials,
                    buffer_size=self.buffer_size,
                    bulk_commit=self.bulk_commit,
                    query=input.query))
              
            database_outputs = []
            for output in rule.outputs:
                database_outputs.append(DatabaseFactory().create(
                    output.credentials,
                    buffer_size=self.buffer_size,
                    bulk_commit=self.bulk_commit,
                    table=Table(output.table, 0),
                    table_name=output.table,
                    ))
                
            
            module.exec(database_inputs, database_outputs)
            DatabaseFactory().close_all_connections()

            end_time = time.perf_counter()
            execution_time_ms = int((end_time - start_time) * 1000)
            log(Level.INFO, f"[data_migration] Rule {rule.name} executed in {execution_time_ms} ms\n")
            
            
        
           
            
        
        


