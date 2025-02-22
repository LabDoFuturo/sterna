import time
import random


def format_reserved_word(word):
    """
    Formats a word by enclosing it in double quotes if it is a database reserved word.
    
    """
    reserved_words = {'NAME', 'DEFAULT', 'ORDER', 'TYPE', 'KEY'}

    if word.upper() in reserved_words:
        return f'"{word}"'
    else:
        return word
    
    
def convert_value_to_numeric(value):
    """
    Converts a given value to a numeric type if possible.

    Attempts to convert the input value to a float. If the float is an integer,
    returns it as an int; otherwise, returns it as a float. If the conversion
    fails, returns the original value.
    """
    try:
        float_val = float(value)
        return int(float_val) if float_val.is_integer() else float_val
    except ValueError:
        return value
    


def unique_timestamp_string_id():
    """
    Generates a unique ID based on the current timestamp and a random number.
    
    """
    return f"{int(time.time() * 1000):x}{random.randint(0, 255):02x}"
