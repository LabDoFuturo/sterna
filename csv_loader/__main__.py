import traceback
import time
import sys

from system_logging.log_manager import log, Level
from configs.yaml_manager import load_csv_loader
from csv_loader.csv_to_database import csv_importer


if __name__ == "__main__":
    try:
        start_time = time.perf_counter()
        
        csv_importer(**load_csv_loader())
        
        end_time = time.perf_counter()
        execution_time_seconds = end_time - start_time
        execution_time_ms = int(execution_time_seconds * 1000)
        log(Level.INFO, f"[csv_loader] execution time: {execution_time_ms} ms")
    except Exception as e:
        log(Level.ERROR, f'[csv_loader] Fatal error: {e}')
        log(Level.ERROR, traceback.format_exc())
        sys.exit(1)
