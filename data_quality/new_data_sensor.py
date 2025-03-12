import sys
import traceback
import time

from configs.generic_module import GenericModule
from configs.yaml_manager import load_credentials, get_private_folder, load_yaml_file, save_sensor_file, load_sensor_file
from system_logging.log_manager import log, Level
from data_access.db_factory import DatabaseFactory

class NewDataModule(GenericModule):
    def __init__(self):
        super().__init__("New Data Sensor")
        
    def run(self, configs: dict):
        try:
            
            credentials = load_credentials(configs=configs)
            private_folder = get_private_folder()
            sensor_data = load_sensor_file()
            new_sensor_data = {}
            for key in credentials:
                print(f"[new_data_sensor] database: {key}")
                db = DatabaseFactory().create(credentials[key], buffer_size=1000)
                db.create_connection(reuse=False)
                tables = db.tables_names()
                for table in tables:
                    count = db.metadata(table_name=table).get_table_row_count()
                    #print(f"[new_data_sensor] table: {table}, count: {count}")
                    sensor_key = f"{key}.{table}"
                    new_sensor_data[sensor_key] = count
                    old_count = sensor_data.get(sensor_key, count)
                    diff = count - old_count
                    if diff > 0:
                        print(f"[new_data_sensor] table: {table} => +{diff} rows")
                    elif diff < 0:
                        print(f"[new_data_sensor] table: {table} => {diff} rows")
                    #else:
                        #print(f"[new_data_sensor] table: {table} => 0 rows")
                
                save_sensor_file(new_sensor_data)
                db.close_connection()
        
        except Exception as e:
            log(Level.ERROR, f'[new_data_sensor] Fatal error: {e}')
            log(Level.ERROR, traceback.format_exc())
            sys.exit(1)

if __name__ == "__main__":
    NewDataModule().execute()
