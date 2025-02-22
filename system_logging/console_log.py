class ConsoleLog:
    """
    A class to handle logging messages to the console.

    Attributes:
        levels (list): A list of log filtered levels to be recorded.
    """

    def __init__(self, levels):
        self.levels = levels
        
    def record(self, level, msg):
        """
        Records a log message to the console if the log level is in the specified levels.

        Args:
            level (str): The log level of the message.
            msg (str): The log message to record.
        """
        if level in self.levels:
            print(f"[{level}] {msg}")