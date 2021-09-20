#!/usr/bin/env python

"""Tests for `geoso` package."""


import unittest
from click.testing import CliRunner

import geoso


class Test_Geoso(unittest.TestCase):
    """Tests for `geoso` package."""

    def setUp(self):
        """Set up test fixtures, if any."""

    def tearDown(self):
        """Tear down test fixtures, if any."""

    def test_version(self):
        """Test the version of the package"""
        assert geoso.__version__ == '0.0.1'

    # TODO: Add command line test
    # def test_command_line_interface(self):
    #     """Test the CLI."""
    #     runner = CliRunner()
    #     result = runner.invoke(cli, [])
    #     assert result.exit_code == 0
    #     assert 'export_postgres_to_csv' in result.output
        # help_result = runner.invoke(cli.main, ['--help'])
        # assert help_result.exit_code == 0
        # assert '--help  Show this message and exit.' in help_result.output
