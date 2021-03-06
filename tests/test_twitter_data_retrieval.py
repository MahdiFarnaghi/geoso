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


class Test_Twitter_data_retrieval(unittest.TestCase):
    """Tests for `geoso.twitter` module."""

    def setUp(self) -> None:
        self.DB_HOSTNAME, self.DB_PORT, self.DB_USERNAME, self.DB_PASSWORD, self.DB_DATABASE, self.DB_SCHEMA = EnvVar.get_test_db_env_variables()
        drop_create_database(self.DB_HOSTNAME, self.DB_PORT, self.DB_USERNAME,
                             self.DB_PASSWORD, self.DB_DATABASE, self.DB_SCHEMA)

    def tearDown(self):
        drop_database(self.DB_HOSTNAME, self.DB_PORT, self.DB_USERNAME,
                      self.DB_PASSWORD, self.DB_DATABASE, self.DB_SCHEMA)

    def test_twitter_retrieve_data_streaming_api_save_to_file(self):
        """Test the twitter_retrieve_data_streaming_api function of the package"""

        consumer_key, consumer_secret, access_token, access_secret = EnvVar.get_test_twitter_credentials_env_variables()

        save_data_mode = 'FILE'
        tweets_output_folder = Folders.get_temp_folder()
        area_name = 'London'
        x_min = -1
        x_max = 1
        y_min = 51
        y_max = 52
        languages = 'en'
        max_num_tweets = 5
        only_geotagged = False
        verbose = True

        twitter_retrieve_data_streaming_api(consumer_key=consumer_key, consumer_secret=consumer_secret, access_token=access_token, access_secret=access_secret, save_data_mode=save_data_mode,
                                            tweets_output_folder=tweets_output_folder, area_name=area_name,  x_min=x_min, x_max=x_max, y_min=y_min, y_max=y_max,
                                            languages=languages, max_num_tweets=max_num_tweets, only_geotagged=only_geotagged, verbose=verbose)

        num_tweets_in_file = 0
        pathlist = Path(tweets_output_folder).glob('**/*.json*')
        for path in pathlist:
            path_in_str = str(path)
            with open(path_in_str, 'r') as f:
                lines = f.readlines()
                for line in lines:
                    if line.strip() != '':
                        num_tweets_in_file += 1
        try:
            shutil.rmtree(tweets_output_folder)
        except:
            pass

        print(F"Number of tweets saved in the FILE: {num_tweets_in_file}")
        assert num_tweets_in_file == max_num_tweets

    def test_twitter_retrieve_data_streaming_api_save_to_db(self):
        """Test the twitter_retrieve_data_streaming_api function of the package"""

        consumer_key, consumer_secret, access_token, access_secret = EnvVar.get_test_twitter_credentials_env_variables()

        save_data_mode = 'DB'
        tweets_output_folder = ''
        area_name = 'London'
        x_min = -1
        x_max = 1
        y_min = 51
        y_max = 52
        languages = 'en'
        max_num_tweets = 10
        only_geotagged = False
        verbose = True
        twitter_retrieve_data_streaming_api(consumer_key=consumer_key, consumer_secret=consumer_secret, access_token=access_token, access_secret=access_secret, save_data_mode=save_data_mode,
                                            tweets_output_folder=tweets_output_folder, area_name=area_name,  x_min=x_min, x_max=x_max, y_min=y_min, y_max=y_max,
                                            languages=languages, max_num_tweets=max_num_tweets, only_geotagged=only_geotagged,
                                            db_database=self.DB_DATABASE,
                                            db_hostname=self.DB_HOSTNAME,
                                            db_password=self.DB_PASSWORD,
                                            db_port=self.DB_PORT,
                                            db_schema=self.DB_SCHEMA,
                                            db_username=self.DB_USERNAME,
                                            verbose=verbose)
        self.postgres = PostgresHandler_Tweets(
            self.DB_HOSTNAME, self.DB_PORT, self.DB_DATABASE, self.DB_USERNAME, self.DB_PASSWORD, self.DB_SCHEMA)

        print(
            F"Number of tweets saved in the DB: {self.postgres.number_of_tweets()}")
        # assert self.postgres.number_of_tweets() == max_num_tweets
        assert self.postgres.number_of_tweets() > 0
