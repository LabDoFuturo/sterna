import yaml
from data_access.db_credentials import DBCredentials
from system_logging.console_log import ConsoleLog
from data_migration.rule import Rule, Input, Output

PRIVATE_FOLDER = "private"
RULES_FOLDER = f"{PRIVATE_FOLDER}/rules"
GENERAL_CONFIGS_FILE = f"{PRIVATE_FOLDER}/configs.yml"


def update_private_folder(folder):
    global PRIVATE_FOLDER
    global RULES_FOLDER
    global GENERAL_CONFIGS_FILE
    PRIVATE_FOLDER = folder
    RULES_FOLDER = f"{PRIVATE_FOLDER}/rules"
    GENERAL_CONFIGS_FILE = f"{PRIVATE_FOLDER}/configs.yml"
    
def get_rules_folder():
    return RULES_FOLDER

def load_yaml_file(file_path):
    configs = None
    try:
        with open(file_path, 'r') as file:
            configs = yaml.safe_load(file)
        return configs
    except Exception as e:
        raise e
    
def load_credentials(configs=None):
    if configs is None:
        configs = load_yaml_file(GENERAL_CONFIGS_FILE)
        
    databases_connections = configs['databases_connections']
    if databases_connections is None:
        return None
    db_credentials = {}
    for name, db in databases_connections.items():
        db_credentials[name] = DBCredentials(name, db['database'], db['user'], db['password'], db['host'], db['port'], db['schema'], db['type'])
    return db_credentials

def load_csv_loader(configs=None):
    if configs is None:
        configs = load_yaml_file(GENERAL_CONFIGS_FILE)
    csv_loader = configs.get('csv_loader')
    
    target_database = csv_loader['target_database']
    if target_database is None:
        raise Exception('No target database found')
    
    credentials = load_credentials(configs=configs)
    if credentials is None:
        raise Exception('No credentials found')
    
    target_credentials = credentials.get(target_database)
    if target_credentials is None:
        raise Exception(f'No credentials found for target database {target_database}')
    
    buffer_size = csv_loader.get('buffer_size', 1000)
    bulk_commit = csv_loader.get('bulk_commit', False)
    csv_files = csv_loader['csv_files']
    
    return {
        'credentials': target_credentials,
        'buffer_size': buffer_size,
        'bulk_commit': bulk_commit,
        'csv_files': csv_files
    }



def load_system_logging(log_manager,configs=None):
    if configs is None:
        configs = load_yaml_file(GENERAL_CONFIGS_FILE)
    system_logging = configs['system_logging']
    if system_logging is None:
        raise Exception('No system logging found')
    log_manager.observers = []
    for name, log in system_logging.items():
        if name == 'console_log':
            levels = log['levels']
            log_manager.add_observer(ConsoleLog(levels))
                  
def load_data_migration(mapper,configs=None):
    if configs is None:
        configs = load_yaml_file(GENERAL_CONFIGS_FILE)
             
    data_migration = configs.get('data_migration', {})
    rules = data_migration.get('rules', {})
    
    credentials = load_credentials(configs=configs)
    if credentials is None:
        raise Exception('No credentials found')
    
    mapper.buffer_size = data_migration.get('buffer_size', 1000)
    mapper.bulk_commit = data_migration.get('bulk_commit', False)
    mapper.rules = []
    
    for name, rule in rules.items():
        if not isinstance(rule, dict):
            raise Exception(f"Rule '{name}' is empty or badly formatted.")

        inputs = rule.get("inputs", None)
        outputs = rule.get("outputs", None)

        # Inputs not found
        if inputs is None:
            raise Exception(f"Rule '{name}' is missing the 'inputs' section.")
        # Outputs not found
        if outputs is None:
            raise Exception(f"Rule '{name}' is missing the 'outputs' section.")
        
        # Inputs is not a dictionary or is empty
        if inputs is not None and (not isinstance(inputs, dict) or not inputs):
            raise Exception(f"Rule '{name}' has an invalid or empty 'inputs' section.")
        # Outputs is not a dictionary or is empty
        if outputs is not None and (not isinstance(outputs, dict) or not outputs):
            raise Exception(f"Rule '{name}' has an invalid or empty 'outputs' section.")

        rule_obj = Rule(name)
        mapper.rules.append(rule_obj)
        
        for db, queries in (inputs or {}).items():
            if not queries or not isinstance(queries, list):
                raise Exception(f"Rule '{name}' has an empty input database '{db}'.")
            
            db_credentials = credentials.get(db)
            if db_credentials is None:
                raise Exception(f'No credentials found for target database {db}')
            
            for query in queries:
                rule_obj.inputs.append(Input(db_credentials, query))
            
        for db, tables in (outputs or {}).items():
            if not tables or not isinstance(tables, list):
                raise Exception(f"Rule '{name}' has an empty output database '{db}'.")
            
            db_credentials = credentials.get(db)
            if db_credentials is None:
                raise Exception(f'No credentials found for target database {db}')
            
            for table in tables:
                rule_obj.outputs.append(Output(db_credentials, table))
        
        
        