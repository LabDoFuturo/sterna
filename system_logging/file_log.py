import os

class FileLog:
    def __init__(self, levels, filename):
        self.levels = levels
        self.file = open(filename, 'a', encoding='utf-8')
        self.fd = self.file.fileno() 

    def record(self, level, msg):
        if level in self.levels:
            self.file.write(f"[{level}] {msg}\n")
            self.file.flush()           
            os.fsync(self.fd)           

    def close(self):
        if not self.file.closed:
            self.file.close()

    def __del__(self):
        self.close()