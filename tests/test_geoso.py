#!/usr/bin/env python

"""Tests for `geoso` package."""
import unittest
from click.testing import CliRunner
import geoso
from geoso import twitter_import_jsonl_file_to_postgres, cli
from geoso.utils import EnvVar, Folders
from test_helper import drop_create_database, drop_database, test_data_jsonl_folder_path, test_data_jsonl_file_path


class Test_Geoso(unittest.TestCase):
    """Tests for `geoso` package."""

    def setUp(self) -> None:
        self.DB_HOSTNAME, self.DB_PORT, self.DB_USERNAME, self.DB_PASSWORD, self.DB_DATABASE, self.DB_SCHEMA = EnvVar.get_test_db_env_variables()
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
            ['--help'],
            ['--verbose', 'test-cli'],
            ['--verbose', 'test-cli', '--message', 'test message'],
            # ['--verbose', 'twitter-get-tweets-information-in-database', '--start_date', '2019-01-01', '--end_date', '2019-12-30', '--min_x', '-180',
            #  '--min_y', '-90', '--max_x', '180', '--max_y', '90', '--db_username', self.DB_USERNAME, '--db_password', self.DB_PASSWORD, '--db_hostname',
            #  self.DB_HOSTNAME,
            #  '--db_port', self.DB_PORT, '--db_database', self.DB_DATABASE, '--db_schema', self.DB_SCHEMA]
        ]
        for command in commands:
            result = runner.invoke(cli.main, command)
            assert result.exit_code == 0
            print(result.stdout)
        
        
        # assert '--help' in result.output
        # assert 'Executing ...' in result.output
        # assert 'Execution finished successfully.' in result.output

    # def test_command_line_interface(self):
    #     """Test the CLI."""

    #     # TODO: Replace with the CLI counterpart
    #     num_inserted = twitter_import_jsonl_file_to_postgres(
    #         test_data_jsonl_file_path,
    #         continue_on_error=True,
    #         db_database=self.DB_DATABASE,
    #         db_hostname=self.DB_HOSTNAME,
    #         db_password=self.DB_PASSWORD,
    #         db_port=self.DB_PORT,
    #         db_schema=self.DB_SCHEMA,
    #         db_username=self.DB_USERNAME)

    #     assert num_inserted > 0

    #     result = runner.invoke(cli.main, ['--verbose',
    #                                       'twitter_get_tweets_information_in_database',
    #                                       '--start_date', '2019-01-01',
    #                                       '--end_date', '2019-12-30',
    #                                       '--min_x', -180,
    #                                       '--min_y', -90,
    #                                       '--max_x', 180,
    #                                       '--max_y', 90,
    #                                       '--db_username', self.DB_USERNAME,
    #                                       '--db_password', self.DB_PASSWORD,
    #                                       '--db_hostname', self.DB_HOSTNAME,
    #                                       '--db_port', self.DB_PORT,
    #                                       '--db_database', self.DB_DATABASE,
    #                                       '--db_schema', self.DB_SCHEMA])

    #     assert result.exit_code == 0
    #     assert 'twitter_get_tweets_information_in_database' in result.output

    #     help_result = runner.invoke(cli.main, ['--help'])
    #     assert help_result.exit_code == 0
    #     assert '--help  Show this message and exit.' in help_result.output


# def test_simple_cloud_commands(ControllerClient, format_result):
#     commands = [
#         ["claim-node", "token"],
#         ["create-node", "test"],
#         ["delete-node", "test"],
#         ["describe-node", "test"],
#         ["group-add-node", "group", "node"],
#         ["help"],
#         ["list-groups"],
#         ["list-nodes"],
#         ["login"],
#         ["logout"],
#         ["rename-node", "node", "node"]
#     ]

#     format_result.return_value = "result"

#     runner = CliRunner()
#     for command in commands:
#         result = runner.invoke(cloud.root, command, obj={})
#         print("Command {} exit code {}".format(command[0], result.exit_code))
#         assert result.exit_code == 0
