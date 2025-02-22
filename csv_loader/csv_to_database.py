import pandas as pd
from system_logging.log_manager import log, Level
from data_access.db_factory import DatabaseFactory
from data_access.metadata_models import Table
from csv_loader.csv_process_tuple import process_row

def csv_importer(credentials=None, buffer_size=1000, bulk_commit=False, csv_files=None):
    """
    Imports data from CSV files into a PostgreSQL database.

    Args:
        credentials (DBCredentials): The database credentials.
        buffer_size (int, optional): The size of the buffer for bulk inserts. Defaults to 1000.
        bulk_commit (bool, optional): Whether to commit transactions in bulk. Defaults to False.
        csv_files (list, optional): A list of dictionaries containing CSV file information. Each dictionary should have the following keys:
            - path (str): The path to the CSV file.
            - target_table (str): The name of the target table in the database.
            - encoding (str, optional): The encoding of the CSV file. Defaults to "utf-8".
            - delimiter (str, optional): The delimiter used in the CSV file. Defaults to ",".
            - quotechar (str, optional): The character used to quote fields in the CSV file. Defaults to '"'.
            - replace_columns_values (dict, optional): A dictionary of column names and their replacement values. Defaults to None.
    """
    if csv_files is None:
        raise Exception('[csv_loader] No CSV files found')

    
    if bulk_commit:
        log(Level.DEBUG, f'[csv_loader] Bulk commit enabled with buffer size: {buffer_size}')
    
    db = DatabaseFactory().create(credentials, buffer_size=buffer_size, bulk_commit=bulk_commit)
    
    try:
        db.create_connection()
        
        log(Level.DEBUG, '[csv_loader] Starting CSV import')
        writer = None
        for csv_file in csv_files:
            path = csv_file["path"]
            target_table = csv_file["target_table"]
            encoding = csv_file.get("encoding", "utf-8")
            delimiter = csv_file.get("delimiter", ",")
            quotechar = csv_file.get("quotechar", '"')
            replace_columns_values = csv_file.get("replace_columns_values", None)
            
            total_valid = 0
            total = 0
            
            #log(Level.INFO, f'[csv_loader] Importing {target_table}')
            
            df = pd.read_csv(path, delimiter=delimiter, quotechar=quotechar, encoding=encoding)

            meta = db.metadata(table_name=target_table)
            
            table = Table(target_table, 0)
            table.columns = meta.get_table_columns()
            if table.columns is None or len(table.columns) == 0:
                raise Exception(f'[csv_loader] Error: No columns found for table {target_table} in database {credentials.database}')    
            
            writer = db.writer(table=table)
              
            for index, row in df.iterrows():
                p_row = process_row(row, table.columns, replace_columns_values)
                writer.insert(p_row)
                total_valid = total_valid + 1
            
            total = len(df)
            
            writer.flush_buffer()
            writer.commit()
            
            log(Level.DEBUG, f"[csv_loader] Total: {total}")
            log(Level.DEBUG, f"[csv_loader] Total valid lines: {total_valid}")
            if total != total_valid:
                log(Level.ERROR, f"[csv_loader] Error: {total - total_valid} invalid lines found")
            else:
                log(Level.INFO, f"[csv_loader] ---------> {target_table} imported successfully total: {total_valid}")
                    
    except Exception as e:
        if writer:
            writer.rollback()
        raise e
    finally:
        if db is not None:
            db.close_connection()
