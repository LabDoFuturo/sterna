class Rule:
    def __init__(self, name):
        self.name = name
        self.inputs = [] 
        self.outputs = []
        self.skip = False

    def __str__(self):
        inputs_str = "\n".join(str(inp) for inp in self.inputs)
        outputs_str = "\n".join(str(out) for out in self.outputs)
        return f"Rule: {self.name}\nInputs:\n{inputs_str}\nOutputs:\n{outputs_str}\n"


class Input:
    def __init__(self, credentials, query):
        self.credentials = credentials
        self.query = query

    def __str__(self):
        return f"  {self.credentials.name}: {self.query}"


class Output:
    def __init__(self, credentials, table):
        self.credentials = credentials
        self.table = table

    def __str__(self):
        return f"  {self.credentials.name}: {self.table}"

