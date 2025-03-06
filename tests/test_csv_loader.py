import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
from data_access.db_credentials import DBCredentials
from csv_loader.csv_to_database import csv_importer  


class TestCSVLoader(unittest.TestCase):
    
    def setUp(self):   
      self.credentials = DBCredentials(
          name="teste_db",
          host="localhost",
          port=5432,
          user="your_username",
          password="you_password",
          database="test_database",
          schema="public",
          type="postgresql"
      )

      self.csv_files = [
          {
              "path": "test1.csv",
              "target_table": "test_table",
              "encoding": "utf-8",
              "delimiter": ",",
              "quotechar": '"',
              "replace_columns_values": None 
          },
          {
              "path": "test2.csv",
              "target_table": "test2_table",
              "encoding": "utf-8",
              "delimiter": ",",
              "quotechar": '"',
              "replace_columns_values": None 
          }
      ]
      

    def test_csv_loader_with_no_files(self):              
        with self.assertRaises(Exception) as context:
            csv_importer(credentials={}, csv_files=None)        
        self.assertEqual(str(context.exception), "[csv_loader] No CSV files found")
    
    @patch("csv_loader.csv_to_database.pd.read_csv")
    @patch("csv_loader.csv_to_database.process_row")
    @patch("data_access.db_factory.DatabaseFactory.create")
    def test_csv_loader_success(self, mock_db_create, mock_process_row, mock_read_csv):
        mock_db = MagicMock()
        mock_writer = MagicMock()
        mock_db.writer.return_value = mock_writer
        mock_db_create.return_value = mock_db

        mock_db.metadata.return_value.get_table_columns.return_value = ["id", "name", "email"]

        mock_read_csv.return_value = pd.DataFrame([
            {"id": 6786, "name": "João Guimarães", "email": "jg@example.com"},
            {"id": 86756, "name": "Maria Cristina", "email": "mc@example.com"} 
        ])

        mock_process_row.side_effect = lambda row, columns, replace : row
        
        csv_importer(credentials=self.credentials, csv_files=self.csv_files)

        mock_db.create_connection.assert_called_once()
        mock_db.writer.assert_called()
        self.assertEqual(mock_writer.insert.call_count, 4)
        mock_writer.flush_buffer.assert_called()
        mock_writer.commit.assert_called()
        mock_db.close_connection.assert_called_once()

    @patch("csv_loader.csv_to_database.pd.read_csv")
    @patch("data_access.db_factory.DatabaseFactory.create")   
    def test_csv_loader_no_columns(self, mock_db_create, mock_read_csv):
      mock_db = MagicMock()
      mock_writer = MagicMock()

      mock_db.writer.return_value = mock_writer
      mock_db_create.return_value = mock_db

      mock_db.metadata.return_value.get_table_columns.return_value = []

      mock_read_csv.return_value = pd.DataFrame([
            {"id": 6786, "name": "João Guimarães", "email": "jg@example.com"},
            {"id": 86756, "name": "Maria Cristina", "email": "mc@example.com"} 
        ]) 
      
      with self.assertRaises(Exception) as context:
        csv_importer(credentials=self.credentials, csv_files=self.csv_files)       
      self.assertTrue("[csv_loader] Error: No columns found for table" in str(context.exception))  

if __name__ == "__main__":
    unittest.main()
