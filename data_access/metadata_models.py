class Table:
    """
    A class to represent a database table.

    Attributes:
        name (str): The name of the table.
        num_tuples (int): The number of tuples (rows) in the table.
        columns (list): A list of Column objects representing the columns of the table.
        constraints (list): A list of Constraint objects representing the constraints of the table.
        indexes (list): A list of Index objects representing the indexes of the table.
    """

    def __init__(self, name, num_tuples, columns=None, constraints=None, indexes=None):
        self.name = name
        self.num_tuples = num_tuples
        self.columns = columns or []
        self.constraints = constraints or []
        self.indexes = indexes or []
    
    def __str__(self):
        return f"Table(name={self.name}, num_tuples={self.num_tuples}, columns={self.columns}, constraints={self.constraints}, indexes={self.indexes})"

class Column:
    """
    A class to represent a column in a database table.

    Attributes:
        name (str): The name of the column.
        data_type (str): The data type of the column.
        nullable (bool): Whether the column can contain null values.
        default (str): The default value of the column.
    """

    def __init__(self, name, data_type=None, nullable=None, default=None):
        self.name = name
        self.data_type = data_type
        self.nullable = nullable
        self.default = default

    def __str__(self):
        return f"Column(name={self.name}, data_type={self.data_type}, nullable={self.nullable}, default={self.default})"

class Constraint:
    """
    A class to represent a constraint on a database table.

    Attributes:
        name (str): The name of the constraint.
        column_name (str): The name of the column the constraint applies to.
        referenced_table_schema (str): The schema of the referenced table.
        referenced_table_name (str): The name of the referenced table.
        referenced_column_name (str): The name of the referenced column.
    """

    def __init__(self, name, column_name, referenced_table_schema, referenced_table_name, referenced_column_name):
        self.name = name
        self.column_name = column_name
        self.referenced_table_schema = referenced_table_schema
        self.referenced_table_name = referenced_table_name
        self.referenced_column_name = referenced_column_name

class Index:
    """
    A class to represent an index on a database table.

    Attributes:
        name (str): The name of the index.
        column_name (str): The name of the column the index applies to.
        nullable (bool): Whether the column can contain null values.
        index_type (str): The type of the index.
        non_unique (bool): Whether the index allows non-unique values.
    """
    def __init__(self, name, column_name, nullable, index_type, non_unique):   
        self.name = name
        self.column_name = column_name
        self.nullable = nullable
        self.index_type = index_type
        self.non_unique = non_unique