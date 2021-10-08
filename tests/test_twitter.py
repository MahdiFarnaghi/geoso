from geoso.postgres import PostgresHandler_Tweets
import unittest
import os
from pathlib import Path
import csv
import traceback
import sqlalchemy_utils
from sqlalchemy_utils.functions.database import drop_database
from geoso import utils
import shutil

from geoso.utils import EnvVar, Folders
from geoso import twitter_import_jsonl_folder_to_db, twitter_import_jsonl_folder_to_db, twitter_export_db_to_csv, twitter_retrieve_data_streaming_api, twitter_import_jsonl_file_to_db, twitter_get_tweets_info_from_db
from test_helper import drop_create_database, drop_database, test_data_jsonl_folder_path, test_data_jsonl_file_path


class Test_Twitter(unittest.TestCase):
    """Tests for `geoso.twitter` module."""

    def setUp(self) -> None:
        self.DB_HOSTNAME, self.DB_PORT, self.DB_USERNAME, self.DB_PASSWORD, self.DB_DATABASE, self.DB_SCHEMA = EnvVar.get_test_db_env_variables()
        drop_create_database(self.DB_HOSTNAME, self.DB_PORT, self.DB_USERNAME,
                             self.DB_PASSWORD, self.DB_DATABASE, self.DB_SCHEMA)

    def tearDown(self):
        drop_database(self.DB_HOSTNAME, self.DB_PORT, self.DB_USERNAME,
                      self.DB_PASSWORD, self.DB_DATABASE, self.DB_SCHEMA)

    def test_jsonl_folder_to_db(self):

        num_inserted = twitter_import_jsonl_folder_to_db(
            test_data_jsonl_folder_path,
            continue_on_error=True,
            db_database=self.DB_DATABASE,
            db_hostname=self.DB_HOSTNAME,
            db_password=self.DB_PASSWORD,
            db_port=self.DB_PORT,
            db_schema=self.DB_SCHEMA,
            db_username=self.DB_USERNAME)

        assert num_inserted > 0

    def test_get_tweets_info_from_db(self):
        num_inserted = twitter_import_jsonl_file_to_db(
            test_data_jsonl_file_path,
            continue_on_error=True,
            db_database=self.DB_DATABASE,
            db_hostname=self.DB_HOSTNAME,
            db_password=self.DB_PASSWORD,
            db_port=self.DB_PORT,
            db_schema=self.DB_SCHEMA,
            db_username=self.DB_USERNAME)

        assert num_inserted > 0

        start_date = '2019-01-01'
        end_date = '2019-12-30'
        x_min = -180
        x_max = 180
        y_min = -90
        y_max = 90

        df = twitter_get_tweets_info_from_db(
            start_date=start_date,
            end_date=end_date,
            x_min=x_min,
            x_max=x_max,
            y_min=x_min,
            y_max=y_max,
            db_database=self.DB_DATABASE,
            db_hostname=self.DB_HOSTNAME,
            db_password=self.DB_PASSWORD,
            db_port=self.DB_PORT,
            db_schema=self.DB_SCHEMA,
            db_username=self.DB_USERNAME)

        assert df.iloc[0, 0] > 0

        df = twitter_get_tweets_info_from_db(
            start_date=start_date,
            end_date=end_date,
            db_database=self.DB_DATABASE,
            db_hostname=self.DB_HOSTNAME,
            db_password=self.DB_PASSWORD,
            db_port=self.DB_PORT,
            db_schema=self.DB_SCHEMA,
            db_username=self.DB_USERNAME)

        assert df.iloc[0, 0] > 0

        df = twitter_get_tweets_info_from_db(
            x_min=x_min,
            x_max=x_max,
            y_min=x_min,
            y_max=y_max,
            db_database=self.DB_DATABASE,
            db_hostname=self.DB_HOSTNAME,
            db_password=self.DB_PASSWORD,
            db_port=self.DB_PORT,
            db_schema=self.DB_SCHEMA,
            db_username=self.DB_USERNAME)

        assert df.iloc[0, 0] > 0

        df = twitter_get_tweets_info_from_db(
            db_database=self.DB_DATABASE,
            db_hostname=self.DB_HOSTNAME,
            db_password=self.DB_PASSWORD,
            db_port=self.DB_PORT,
            db_schema=self.DB_SCHEMA,
            db_username=self.DB_USERNAME)

        assert df.iloc[0, 0] > 0

    def test_export_db_to_csv(self):

        num_inserted = twitter_import_jsonl_folder_to_db(
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
            twitter_export_db_to_csv(file_path=file_path,
                                     start_date='2019-01-01',
                                     end_date='2019-12-30',
                                     x_min=-180,
                                     y_min=-90,
                                     x_max=180,
                                     y_max=90,
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

