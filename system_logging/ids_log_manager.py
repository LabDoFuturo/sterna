from system_logging.log_manager import log, Level

def log_id(schema, table, columns, data, logging_ids_key=None):
    if table is None:
        log(Level.ERROR, f"[ids_log_manager] Table is None")
        return
    if table.columns is None:
        log(Level.ERROR, f"[ids_log_manager] Table columns are None")
        return
    if table.name is None:
        log(Level.ERROR, f"[ids_log_manager] Table name is None")
        return
    if schema is None:
        schema = ""

    key = "id"
    if logging_ids_key:
        key = logging_ids_key

    if columns is None:
        columns = [column.name for column in table.columns]
    find = False
    index = 0
    for c in columns:
        if c == key:
            find = True
            break
        index += 1

    if not find:
        log(Level.ERROR, f"[ids_log_manager] Column {key} not found in table {table.name}")
        return
    
    if isinstance(data, dict):
        data = [data[column.name] for column in table.columns]

    if len(data) == 0:
        log(Level.ERROR, f"[ids_log_manager] Data is empty")
        return
    if len(data) != len(table.columns):
        log(Level.ERROR, f"[ids_log_manager] Data length {len(data)} does not match table columns length {len(table.columns)}")
        return
    if data[0] is None:
        log(Level.ERROR, f"[ids_log_manager] Data is None")
        return
    if data[index] is None:
        log(Level.ERROR, f"[ids_log_manager] Data {key} is None (index: {index})")
        return
    if data[index] == "":
        log(Level.ERROR, f"[ids_log_manager] Data {key} is empty")
        return
    
    log(Level.ID, f"{schema}{table.name}.{key}: {data[index]}")