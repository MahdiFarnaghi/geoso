import unittest
import os
from pathlib import Path

from nltk.corpus.util import TRY_ZIPFILE_FIRST
from geoso.utils import EnvVar

from geoso.reader_writer import TweetReaderWriter
from geoso.postgres import PostgresHandler_Tweets
from geoso.postgres import PostgresHandler
import sqlalchemy_utils

from sqlalchemy.engine import create_engine
import traceback, sys


test_data_path = os.path.join(
    Path(os.path.realpath(__file__)).parent, 'test_data')
test_data_jsonl_folder_path = os.path.join(test_data_path, 'jsonl')

#TODO: Change the implementation to use a local database, e.g., using https://pypi.org/project/pytest-postgresql/ or https://pypi.org/project/testing.postgresql/
class Test_TweetReaderWriter(unittest.TestCase):
    """Tests for `geoso.reader_writer` module."""

    def setUp(self) -> None:
        self.DB_HOSTNAME, self.DB_PORT, self.DB_USERNAME, self.DB_PASSWORD, self.DB_DATABASE, self.DB_SCHEMA = EnvVar.get_db_env_variables()
        # self.DB_DATABASE = 'test_geoso'
        # # TODO: What if .env file is not available or it does not have database connection info
        # self.postgres = PostgresHandler(
        #     self.DB_HOSTNAME, self.DB_PORT, self.DB_DATABASE, self.DB_USERNAME, self.DB_PASSWORD, self.DB_SCHEMA)

        # try:
        #     sqlalchemy_utils.functions.drop_database(self.postgres.db_url)
        # except:
        #     pass
        # pass

        self.postgres = PostgresHandler(
            self.DB_HOSTNAME, self.DB_PORT, self.DB_DATABASE, self.DB_USERNAME, self.DB_PASSWORD, self.DB_SCHEMA)

    def tearDown(self):        
        # try:
        #     sqlalchemy_utils.functions.drop_database(self.postgres.db_url)
        # except:
        #     pass
        pass

    def test_jsonl_folder_to_postgres(self):
        
        num_inserted = TweetReaderWriter.jsonl_folder_to_postgres(
            test_data_jsonl_folder_path,
            db_database=self.DB_DATABASE,
            db_hostname=self.DB_HOSTNAME,
            db_password=self.DB_PASSWORD,
            db_port=self.DB_PORT,
            db_schema=self.DB_SCHEMA,
            db_username=self.DB_USERNAME)
        assert  num_inserted > 0
