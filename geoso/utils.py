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
from colorama import init
from termcolor import cprint
import enum
import shutil

init()

load_dotenv()

class Text_Categories(enum.Enum):
    Process_start = 1
    Process_end = 2
    Process_description = 3
    Process_time = 4
    Error = 5


def dprint(text, verbose=True, text_category=None, level=1):
    if level >= 1:
        _text = ' '*(level - 1) + str(text)
    else:
        _text = text
    if verbose:
        if text_category == Text_Categories.Process_start or text_category == Text_Categories.Process_end:
            cprint(_text, 'green')
        elif text_category == Text_Categories.Process_description:
            cprint(_text, 'grey')
        elif text_category == Text_Categories.Process_time:
            cprint(_text, 'blue')
        elif text_category == Text_Categories.Error:
            cprint(_text, 'red')
        else:
            print(text)


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

    def get_twitter_credentials_env_variables():
        consumer_key = os.getenv('CONSUMER_KEY')
        consumer_secret = os.getenv('CONSUMER_SECRET')
        access_token = os.getenv('ACCESS_TOKEN')
        access_secret = os.getenv('ACCESS_SECRET')
        return consumer_key, consumer_secret, access_token, access_secret

    def get_db_env_variables_if_none(db_hostname, db_port, db_user, db_pass, db_database, db_schema):

        if db_hostname is None or db_hostname == '':
            db_hostname = os.getenv('DB_HOSTNAME')

        if db_port is None or db_port == '':
            db_port = os.getenv('DB_PORT')

        if db_user is None or db_user == '':
            db_user = os.getenv('DB_USER')

        if db_pass is None or db_pass == '':
            db_pass = os.getenv('DB_PASS')

        if db_database is None or db_database == '':
            db_database = os.getenv('DB_DATABASE')

        if db_schema is None or db_schema == '':
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
        return db_hostname, db_port, db_user, db_pass, db_database, db_schema

    @staticmethod
    def get_test_twitter_credentials_env_variables():
        consumer_key = os.getenv('TEST_CONSUMER_KEY')
        consumer_secret = os.getenv('TEST_CONSUMER_SECRET')
        access_token = os.getenv('TEST_ACCESS_TOKEN')
        access_secret = os.getenv('TEST_ACCESS_SECRET')
        return consumer_key, consumer_secret, access_token, access_secret


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
