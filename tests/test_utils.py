#!/usr/bin/env python

"""Tests for `geoso` package."""


import os
from pathlib import Path
import unittest
from click.testing import CliRunner

from geoso.utils import Folders


class Test_Utils(unittest.TestCase):
    """Tests for `geoso` package."""

    def setUp(self):
        """Set up test fixtures, if any."""

    def tearDown(self):
        """Tear down test fixtures, if any."""

    def test_get_temp_folder(self):
        """Test the version of the package"""
        tmp = Folders.get_temp_folder()
        tmp_exists = Path(tmp).exists()
        try:
            os.remove(tmp)
        except:
            pass
        assert tmp_exists

    # TODO: Add command line test
    # def test_command_line_interface(self):
    #     """Test the CLI."""
    #     runner = CliRunner()
    #     result = runner.invoke(cli.main)
    #     assert result.exit_code == 0
    #     assert 'geoso.cli.main' in result.output
    #     help_result = runner.invoke(cli.main, ['--help'])
    #     assert help_result.exit_code == 0
    #     assert '--help  Show this message and exit.' in help_result.output
