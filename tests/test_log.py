import unittest
import yaml
from io import StringIO
from system_logging.log_manager import instance, Level, log
from system_logging.console_log import ConsoleLog
import sys
import io

class TestLog(unittest.TestCase):
    def setUp(self):
        # Set up YAML configuration as a string
        self.config_yaml = """
            system_logging:
                console_log:
                    levels:
                    - INFO
                    - ERROR
                    - WARNING
                    #- DEBUG
        """
        self.configs = yaml.safe_load(StringIO(self.config_yaml))

        # Redirect stdout to capture terminal output
        self.captured_output = io.StringIO()
        sys.stdout = self.captured_output

    def tearDown(self):
        # Restore stdout to its original state after the test
        sys.stdout = sys.__stdout__
        pass

    def test_system_console_log(self):
        # Initialize LogManager with the configurations
        instance().set_configs(self.configs)
        
        find_console_observer = False
        for observer in instance().observers:
            if isinstance(observer, ConsoleLog):
                find_console_observer = True
                break
            
        if not find_console_observer:
            self.skipTest('ConsoleLog observer not found in LogManager') 
            return     
                
        # Generate logs
        log(Level.INFO, 'INFO MSG')
        log(Level.DEBUG, 'DEBUG MSG')  # Should not be displayed since DEBUG is commented out
        log(Level.ERROR, 'ERROR MSG')
        log(Level.WARNING, 'WARNING MSG')

        # Get the captured output
        output = self.captured_output.getvalue()

        # Verify if the expected messages are in the output
        self.assertIn('INFO MSG', output)  # INFO should be present
        self.assertNotIn('DEBUG MSG', output)  # DEBUG should not be present
        self.assertIn('ERROR MSG', output)  # ERROR should be present
        self.assertIn('WARNING MSG', output)  # WARNING should be present
        
        

if __name__ == '__main__':
    unittest.main()