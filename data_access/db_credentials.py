"""
    A class to store database credentials.
"""
class DBCredentials:
    def __init__(self, name, database, user, password, host, port, schema=None, type=None):
        self.name = name
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.schema = schema
        self.type = type
        
    def __str__(self):
        return (
            f"Name: {self.name}\n"
            f"Database: {self.database}\n"
            f"User: {self.user}\n"
            f"Password: {self.password}\n"
            f"Host: {self.host}\n"
            f"Port: {self.port}\n"
            f"Schema: {self.schema}\n"
            f"Type: {self.type}\n"
        )