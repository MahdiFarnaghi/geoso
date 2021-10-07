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
