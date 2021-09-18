from unittest import TestCase
import sqlalchemy_utils

from geoso.utils import EnvVar
from geoso.postgres import PostgresHandler
from test_helper import drop_create_database, drop_database

class Test_PostgresHandler(TestCase):
    """Test for 'geoso.postgres' module. """

    def setUp(self) -> None:
        self.DB_HOSTNAME, self.DB_PORT, self.DB_USERNAME, self.DB_PASSWORD, self.DB_DATABASE, self.DB_SCHEMA = EnvVar.get_test_db_env_variables()
        drop_create_database(self.DB_HOSTNAME, self.DB_PORT, self.DB_USERNAME,
                             self.DB_PASSWORD, self.DB_DATABASE, self.DB_SCHEMA)

        self.postgres = PostgresHandler(
            self.DB_HOSTNAME, self.DB_PORT, self.DB_DATABASE, self.DB_USERNAME, self.DB_PASSWORD, self.DB_SCHEMA)

    def tearDown(self):
        drop_database(self.DB_HOSTNAME, self.DB_PORT, self.DB_USERNAME,
                      self.DB_PASSWORD, self.DB_DATABASE, self.DB_SCHEMA)

    def test_check_db(self):
        self.assertTrue(
            self.postgres.check_db() and self.postgres.db_version == PostgresHandler.expected_db_version)

    # def test_upsert_tweet(self):
    #     path = WorkspacePathSettings.get_path_test_tweet_json_file()
    #     with open(str(path.absolute()), "r") as f:
    #         tweet_text = f.read()
    #         res1 = self.postgres.upsert_tweet(tweet_text, force_insert=False)
    #         res2 = self.postgres.upsert_tweet(tweet_text, force_insert=True)
    #         self.assertTrue(res1 and res2)

    # def test_bulk_insert(self):
    #     path = WorkspacePathSettings.get_path_test_tweet_reads_file()
    #     tweets = []
    #     with open(str(path.absolute()), "r") as f:
    #         for line in f:
    #             if len(tweets) > 100:
    #                 break
    #             if (line.strip()) != "":
    #                 tweets.append(str(line.strip()))
    #     self.postgres.bulk_insert_geotagged_tweets(tweets, force_insert=True)
    #     self.assertTrue(True)
