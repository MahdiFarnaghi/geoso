#!/usr/bin/env python

"""Tests for `geoso` package."""
import os
import unittest
from click.testing import CliRunner
import geoso
from geoso import twitter_import_jsonl_file_to_db, cli
from geoso.utils import EnvVar, Folders
from test_helper import drop_create_database, drop_database, test_data_jsonl_folder_path, test_data_jsonl_file_path


class Test_Geoso(unittest.TestCase):
    """Tests for `geoso` package."""

    def setUp(self) -> None:
        self.DB_HOSTNAME, self.DB_PORT, self.DB_USERNAME, self.DB_PASSWORD, self.DB_DATABASE, self.DB_SCHEMA = EnvVar.get_test_db_env_variables()
        self.consumer_key, self.consumer_secret, self.access_token, self.access_secret = EnvVar.get_test_twitter_credentials_env_variables()
        drop_create_database(self.DB_HOSTNAME, self.DB_PORT, self.DB_USERNAME,
                             self.DB_PASSWORD, self.DB_DATABASE, self.DB_SCHEMA)

    def tearDown(self):
        drop_database(self.DB_HOSTNAME, self.DB_PORT, self.DB_USERNAME,
                      self.DB_PASSWORD, self.DB_DATABASE, self.DB_SCHEMA)

    def test_version(self):
        """Test the version of the package"""
        assert geoso.__version__ == '0.0.1'

    def test_command_line_interface(self):

        runner = CliRunner()
        commands = [
            [['--help'], ['Show this message and exit.']],
            [['--verbose', 'greeting'], ['Execution finished successfully.']],
            [['--verbose', 'greeting', '--name', 'Saosyant'],
                ['Hello Saosyant. Thanks for using geoso.']],
            [['--verbose', 'twitter-import-jsonl-file-to-db',
              '--file_path', test_data_jsonl_file_path, '--continue_on_error',
              '--db_username', self.DB_USERNAME, '--db_password', self.DB_PASSWORD, '--db_hostname',
              self.DB_HOSTNAME, '--db_port', self.DB_PORT,
              '--db_database', self.DB_DATABASE, '--db_schema', self.DB_SCHEMA], ['Execution finished successfully.']],
            [['--verbose', 'twitter-import-jsonl-folder-to-db',
              '--folder_path', test_data_jsonl_folder_path, '--continue_on_error',
              '--db_username', self.DB_USERNAME, '--db_password', self.DB_PASSWORD, '--db_hostname',
              self.DB_HOSTNAME, '--db_port', self.DB_PORT,
              '--db_database', self.DB_DATABASE, '--db_schema', self.DB_SCHEMA], ['Execution finished successfully.']],
            [['--verbose', 'twitter-get-tweets-info-from-db', '--start_date', '2019-01-01', '--end_date', '2019-12-30', '--x_min', '-180',
              '--y_min', '-90', '--x_max', '180', '--y_max', '90',
              '--db_username', self.DB_USERNAME, '--db_password', self.DB_PASSWORD, '--db_hostname',
              self.DB_HOSTNAME, '--db_port', self.DB_PORT,
              '--db_database', self.DB_DATABASE, '--db_schema', self.DB_SCHEMA], ['Execution finished successfully.']],
            [['--verbose', 'twitter-export-db-to-csv', '--file_path', os.path.join(Folders.get_temp_folder(), 'test.csv'),
              '--start_date', '2019-01-01', '--end_date', '2019-12-30', '--x_min', '-180',
              '--y_min', '-90', '--x_max', '180', '--y_max', '90', '--overwrite_file',
              '--db_username', self.DB_USERNAME, '--db_password', self.DB_PASSWORD, '--db_hostname',
              self.DB_HOSTNAME, '--db_port', self.DB_PORT,
              '--db_database', self.DB_DATABASE, '--db_schema', self.DB_SCHEMA], ['Execution finished successfully.']],
            [['--verbose', 'twitter-retrieve-data-streaming-api',
              '--save_data_mode', 'FILE',
              '--tweets_output_folder', Folders.get_temp_folder(),
              '--area_name', 'London',
              '--x_min', '-1', '--y_min', '51', '--x_max', '1', '--y_max', '52',
              '--languages', 'en',
              '--max_num_tweets', '5',
              '--consumer_key', self.consumer_key, '--consumer_secret', self.consumer_secret,
              '--access_token', self.access_token, '--access_secret', self.access_secret], ['Execution finished successfully.']],
        ]
        for command_message in commands:
            command = command_message[0]
            message = command_message[1][0]
            result = runner.invoke(cli.main, command)
            print(result.stdout)
            if not result.exit_code == 0 or not message in result.output:
                print("stdout: ")
                print(result.stdout)
                print("Exception: ")
                print(result.exception)
            assert result.exit_code == 0
            assert message in result.output
