from pathlib import Path
import pytz
from dotenv import load_dotenv
from contextlib import contextmanager
import sys
import os
import tempfile
import traceback
from email.utils import mktime_tz, parsedate_tz
from datetime import datetime

load_dotenv()


class EnvVar:
    @staticmethod
    def get_db_env_variables():
        db_hostname = os.getenv('DB_HOSTNAME')
        db_port = os.getenv('DB_PORT')
        db_user = os.getenv('DB_USER')
        db_pass = os.getenv('DB_PASS')
        db_database = os.getenv('DB_DATABASE')
        db_schema = os.getenv('DB_SCHEMA')
        return db_hostname, db_port, db_user, db_pass, db_database, db_schema

    @staticmethod
    def get_test_db_env_variables():
        db_hostname = os.getenv('TEST_DB_HOSTNAME')
        db_port = os.getenv('TEST_DB_PORT')
        db_user = os.getenv('TEST_DB_USER')
        db_pass = os.getenv('TEST_DB_PASS')
        db_database = os.getenv('TEST_DB_DATABASE')
        db_schema = os.getenv('TEST_DB_SCHEMA')

        # # Default settings of the test database, which is also set for GitHub Postgres service in workflows/build.yml file
        # if db_hostname is None or db_port is None or db_user is None or db_pass is None or db_database is None:
        #     db_hostname = '127.0.0.1'
        #     db_port = 5432
        #     db_user = 'postgres'
        #     db_pass = 'postgres'
        #     db_database = 'geoso_test'
        #     db_schema = 'test'

        return db_hostname, db_port, db_user, db_pass, db_database, db_schema


class Folders:
    @staticmethod
    def get_temp_folder():
        tmp = tempfile.mkdtemp()
        return tmp

    @staticmethod
    def make_dir_with_check(folder_path: str):

        _folder_path = Path(folder_path)

        if not _folder_path.exists():
            os.makedirs(str(_folder_path.absolute()))

    @staticmethod
    def make_parent_dir_with_check(file_path: Path):
        if not Path(file_path).parent.exists():
            Folders.check_make_dir(file_path.parent)


def print_error():
    print('-' * 60)
    print("Unexpected error:", sys.exc_info()[0])
    print('-' * 60)
    traceback.print_exc(file=sys.stdout)
    print('-' * 60)


def parse_datetime_to_system_local_time(value):
    time_tuple = parsedate_tz(value)
    timestamp = mktime_tz(time_tuple)
    return datetime.fromtimestamp(timestamp)


def parse_datetime_to_timezone(value, time_zone):
    return datetime.strptime(value, '%a %b %d %H:%M:%S %z %Y').astimezone(pytz.timezone(time_zone))


@contextmanager
def suppress_stdout():
    with open(os.devnull, "w") as devnull:
        old_stdout = sys.stdout
        sys.stdout = devnull
        try:
            yield
        finally:
            sys.stdout = old_stdout
