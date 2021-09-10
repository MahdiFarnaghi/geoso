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
        pass

    def tearDown(self):                
        pass

    def test_jsonl_folder_to_postgres(self):
        
        num_inserted = TweetReaderWriter.import_from_jsonl_folder_to_postgres(
            test_data_jsonl_folder_path,
            continue_on_error=True,
            db_database=self.DB_DATABASE,
            db_hostname=self.DB_HOSTNAME,
            db_password=self.DB_PASSWORD,
            db_port=self.DB_PORT,
            db_schema=self.DB_SCHEMA,
            db_username=self.DB_USERNAME)
        assert  num_inserted > 0
