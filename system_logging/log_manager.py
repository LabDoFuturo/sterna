from configs.yaml_manager import load_system_logging

class Level:
    """
    A class to define log levels.
    
    """
    ERROR = "ERROR"
    WARNING = "WARNING"
    INFO = "INFO"
    DEBUG = "DEBUG"
    SQL = "SQL"
    ID = "ID"


class LogManager:
    """
    A singleton class to manage logging.

    Attributes:
        _instance (LogManager): The singleton instance of LogManager.
        observers (list): A list of observers to notify with log messages.
    """
    _instance = None

    def __new__(cls):
        """
        Creates a new instance of LogManager if one does not already exist.

        Returns:
            LogManager: The singleton instance of LogManager.
        """
        if cls._instance is None:
            print("Creating LogManager instance")
            cls._instance = super(LogManager, cls).__new__(cls)
            cls._instance.observers = []
            load_system_logging(cls._instance, configs=None)
        return cls._instance
    
    def set_configs(self, configs):
        # print("Setting LogManager configurations")
        load_system_logging(self._instance, configs=configs)
        
    def add_observer(self, observer):
        """
        Adds an observer to the list.

        """
        self.observers.append(observer)

    def remove_observer(self, observer):
        """
        Removes an observer from the list of observers.
        
        """
        self.observers.remove(observer)

    def log(self, level, msg):
        """
        Logs a message to all observers.
        
        """
        for observer in self.observers:
            observer.record(level, msg)
    

def log(level, msg):
    """
    Logs a message using the LogManager singleton instance.
    
    """
    LogManager().log(level, msg)

def instance():
    """
    Returns the LogManager singleton instance.
    
    """
    return LogManager()