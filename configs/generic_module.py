
import sys
import traceback
import time
import argparse
import yaml
from abc import ABC, abstractmethod

class GenericModule(ABC):
    """
    The GenericModule class is an abstract base class that provides a framework for executing a module with a specific configuration.
    It includes methods for parsing command-line arguments, loading configuration files, calculating elapsed time, and executing the module.

    Attributes:
        module_name (str): The name of the module.
        config_path (str): The default path to the configuration file.

    Methods:
        __init__(self, module_name: str): Initializes the module with a name and sets the default configuration file path.
        parse_arguments(self): Parses command-line arguments to get the configuration file path.
        calculate_time(start_time): Calculates and formats the elapsed time since the start time.
        load_configs(self, config_path: str): Loads the configuration file from the specified path.
        run(self, configs: dict): Abstract method that must be implemented by subclasses to define the module's main functionality.
        execute(self): Executes the module by parsing arguments, loading configurations, running the module, and handling errors.
    """
    def __init__(self, module_name: str):
        self.module_name = module_name
        self.config_path = "private/configs.yml"

    def parse_arguments(self):
        parser = argparse.ArgumentParser(description=f"Run {self.module_name} pipeline")
        parser.add_argument(
            "-c", "--config",
            type=str,
            default=self.config_path,
            help=f"Path to configuration file (default: {self.config_path})"
        )
        return parser.parse_args()

    @staticmethod
    def calculate_time(start_time):
        elapsed_time = time.perf_counter() - start_time
        elapsed_time = round(elapsed_time, 2)
        units = [(60, "s"), (60, "m"), (24, "h")]
        for divisor, unit in units:
            if elapsed_time < divisor:
                return f"{elapsed_time}{unit}"
            elapsed_time /= divisor
        return f"{elapsed_time}d"

    def load_configs(self, config_path: str):
        try:
            with open(config_path, "r") as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            print(f"\nError: Configuration file '{config_path}' not found!")
            sys.exit(1)
        except yaml.YAMLError as e:
            print(f"\nError: Invalid YAML syntax in configuration file:\n{e}")
            sys.exit(1)

    @abstractmethod
    def run(self, configs: dict):
        pass

    def execute(self):
        args = self.parse_arguments()
        
        try:
            print(f"\n[{self.module_name}] Using configuration: {args.config}")
            configs = self.load_configs(args.config)
            
            start_time = time.perf_counter()
            print(f"Starting {self.module_name}...")
            
            self.run(configs)
            
            print(f"Finished {self.module_name} in {self.calculate_time(start_time)}")
            
        except Exception as e:
            print(f"\n[{self.module_name}] Critical error:\n{traceback.format_exc()}")
            sys.exit(1)