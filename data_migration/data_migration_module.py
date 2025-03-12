import sys
import traceback
import time

from configs.generic_module import GenericModule
from system_logging.log_manager import log, Level
from data_migration.mapper import Mapper

class DataMigrationModule(GenericModule):
    def __init__(self):
        super().__init__("Data Migration Module")
        
    def run(self, configs: dict):
        try:
            start_time = time.perf_counter()
            
            Mapper().start_migration()
            
            end_time = time.perf_counter()
            execution_time_seconds = end_time - start_time
            execution_time_ms = int(execution_time_seconds * 1000)
            log(Level.INFO, f"[data_migration] execution time: {execution_time_ms} ms")
        except Exception as e:
            log(Level.ERROR, f'[data_migration] Fatal error: {e}')
            log(Level.ERROR, traceback.format_exc())
            sys.exit(1)

if __name__ == "__main__":
    DataMigrationModule().execute()
