import pandas as pd
from data_access.utils import convert_value_to_numeric


def process_row(row, columns, replace_columns_values):
    """
    Processes a row of data by converting values to the appropriate types and replacing specified column values.

    Args:
        row (pd.Series): The row of data to process.
        columns (list): A list of Column objects representing the columns of the table.
        replace_columns_values (dict): A dictionary of column names and their replacement values.
    """
    processed_row = []
    for column in columns:
        if replace_columns_values is not None and column.name in replace_columns_values:
            value = replace_columns_values[column.name]
            processed_row.append(value)
        else:   
            value = row[column.name]
            if pd.isna(value):
                processed_row.append(None)
            elif column.data_type == 'boolean':
                processed_row.append(bool(convert_value_to_numeric(value)))
            else:
                processed_row.append(convert_value_to_numeric(value))
            
    return tuple(processed_row)