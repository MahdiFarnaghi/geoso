import unittest
import os
from pathlib import Path
import csv
import traceback
import sqlalchemy_utils
from sqlalchemy_utils.functions.database import drop_database

from geoso.utils import EnvVar, Folders, print_error
from geoso.reader_writer import TweetReaderWriter
from geoso.postgres import PostgresHandler_Tweets
from geoso.postgres import PostgresHandler
from test_helper import drop_create_database, drop_database


test_data_path = os.path.join(
    Path(os.path.realpath(__file__)).parent, 'test_data')
test_data_jsonl_folder_path = os.path.join(test_data_path, 'jsonl')


class Test_TweetReaderWriter(unittest.TestCase):
    """Tests for `geoso.reader_writer` module."""

    def setUp(self) -> None:
        self.DB_HOSTNAME, self.DB_PORT, self.DB_USERNAME, self.DB_PASSWORD, self.DB_DATABASE, self.DB_SCHEMA = EnvVar.get_test_db_env_variables()
        drop_create_database(self.DB_HOSTNAME, self.DB_PORT, self.DB_USERNAME,
                             self.DB_PASSWORD, self.DB_DATABASE, self.DB_SCHEMA)

    def tearDown(self):
        drop_database(self.DB_HOSTNAME, self.DB_PORT, self.DB_USERNAME,
                      self.DB_PASSWORD, self.DB_DATABASE, self.DB_SCHEMA)

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
            success = cnt_rows == num_inserted

            try:
                os.remove(file_path)
                os.rmdir(Path(file_path).parent)
            except:
                pass

            assert success
