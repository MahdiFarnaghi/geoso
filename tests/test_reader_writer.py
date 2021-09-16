import unittest
import os
from pathlib import Path

from nltk.corpus.util import TRY_ZIPFILE_FIRST
from geoso.utils import EnvVar, Folders

from geoso.reader_writer import TweetReaderWriter
from geoso.postgres import PostgresHandler_Tweets
from geoso.postgres import PostgresHandler
import sqlalchemy_utils

from sqlalchemy.engine import create_engine
import traceback
import sys
import csv


test_data_path = os.path.join(
    Path(os.path.realpath(__file__)).parent, 'test_data')
test_data_jsonl_folder_path = os.path.join(test_data_path, 'jsonl')


class Test_TweetReaderWriter(unittest.TestCase):
    """Tests for `geoso.reader_writer` module."""

    def setUp(self) -> None:
        self.DB_HOSTNAME, self.DB_PORT, self.DB_USERNAME, self.DB_PASSWORD, self.DB_DATABASE, self.DB_SCHEMA = EnvVar.get_test_db_env_variables()

        self.postgres = PostgresHandler(
            self.DB_HOSTNAME, self.DB_PORT, self.DB_DATABASE, self.DB_USERNAME, self.DB_PASSWORD, self.DB_SCHEMA)

        try:
            sqlalchemy_utils.functions.drop_database(self.postgres.db_url)
        except:
            pass
        pass

        self.postgres = PostgresHandler(
            self.DB_HOSTNAME, self.DB_PORT, self.DB_DATABASE, self.DB_USERNAME, self.DB_PASSWORD, self.DB_SCHEMA)

    def tearDown(self):
        try:
            sqlalchemy_utils.functions.drop_database(self.postgres.db_url)
        except:
            pass
        pass

    def test_jsonl_folder_to_postgres(self):

        num_inserted = TweetReaderWriter.import_jsonl_folder_to_postgres(
            test_data_jsonl_folder_path,
            continue_on_error=True,
            db_database=self.DB_DATABASE,
            db_hostname=self.DB_HOSTNAME,
            db_password=self.DB_PASSWORD,
            db_port=self.DB_PORT,
            db_schema=self.DB_SCHEMA,
            db_username=self.DB_USERNAME)
        assert num_inserted > 0

    def test_export_postgres_to_csv(self):
        num_inserted = TweetReaderWriter.import_jsonl_folder_to_postgres(
            test_data_jsonl_folder_path,
            continue_on_error=True,
            db_database=self.DB_DATABASE,
            db_hostname=self.DB_HOSTNAME,
            db_password=self.DB_PASSWORD,
            db_port=self.DB_PORT,
            db_schema=self.DB_SCHEMA,
            db_username=self.DB_USERNAME)

        if num_inserted <= 0:
            assert False
        else:
            file_path = os.path.join(Folders.get_temp_folder(), 'test.csv')
            TweetReaderWriter.export_postgres_to_csv(file_path=file_path,
                                                          start_date='2019-01-01',
                                                          end_date='2019-12-30',
                                                          min_x=-180,
                                                          min_y=-90,
                                                          max_x=180,
                                                          max_y=90,
                                                          db_database=self.DB_DATABASE,
                                                          db_hostname=self.DB_HOSTNAME,
                                                          db_password=self.DB_PASSWORD,
                                                          db_port=self.DB_PORT,
                                                          db_schema=self.DB_SCHEMA,
                                                          db_username=self.DB_USERNAME

                                                          )
            cnt_rows = 0
            with open(file_path) as csvFile:
                reader = csv.DictReader(csvFile)
                for row in reader:
                    cnt_rows = cnt_rows + 1
            success = cnt_rows > 0
            
            try:
                os.remove(file_path)
                os.remove(Path(file_path).parent)
            except:
                pass
            
            assert success
