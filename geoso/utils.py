from pathlib import Path

from dotenv import load_dotenv
from contextlib import contextmanager
import sys, os

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


class Folders:
    @staticmethod
    def get_temp_folder():
        p = os.path.join(os.getenv('APPDATA'), 'gttm')
        if not os.path.exists(p):
            os.makedirs(str(p))
        return Path(p)

    @staticmethod
    def make_dir_with_check(folder_path: Path):
        if not folder_path.exists():
            os.makedirs(str(folder_path.absolute()))

    @staticmethod
    def make_parent_dir_with_check(file_path: Path):
        if not Path(file_path).parent.exists():
            Folders.check_make_dir(file_path.parent)



@contextmanager
def suppress_stdout():
    with open(os.devnull, "w") as devnull:
        old_stdout = sys.stdout
        sys.stdout = devnull
        try:
            yield
        finally:
            sys.stdout = old_stdout