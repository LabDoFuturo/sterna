import unittest
from unittest.mock import MagicMock, patch
import psycopg2
from data_access.db_credentials import DBCredentials
from data_access.postgresql_connection import PostgreSQLConnection

class TestMockPostgreSQLConnection(unittest.TestCase):
    def setUp(self):
        self.db_credentials = DBCredentials(
            name="test_db",
            database="testdb",
            user="testuser",
            password="testpassword",
            host="localhost",
            port=5432,
            schema="public",
            type="postgres"
        )

    @patch('psycopg2.connect')
    def test_create_connection_success(self, mock_connect):
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection

        postgres_conn = PostgreSQLConnection(self.db_credentials)
        postgres_conn.create()

        self.assertIsNotNone(postgres_conn.connection)
        mock_connect.assert_called_once_with(
            dbname=self.db_credentials.database,
            user=self.db_credentials.user,
            password=self.db_credentials.password,
            host=self.db_credentials.host,
            port=self.db_credentials.port
        )

if __name__ == '__main__':
    unittest.main()