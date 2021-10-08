from abc import abstractclassmethod, abstractmethod
import os
from datetime import datetime

from tqdm import tqdm
from pathlib import Path

from datetime import datetime

from .utils import suppress_stdout, Folders
from .postgres import PostgresHandler_Tweets
import abc
import gzip
import pathlib
import json
import shutil
import pandas as pd


def twitter_get_tweets_info_from_db(start_date: datetime = None, end_date: datetime = None,
                                    x_min: float = None, y_min: float = None, x_max: float = None, y_max: float = None,
                                    tag: str = None, language: str = None,
                                    db_username='', db_password='', db_hostname='', db_port='', db_database='', db_schema='',
                                    verbose=False) -> pd.DataFrame:
    _start_date = None
    _end_date = None

    if start_date is not None and end_date is not None:
        try:
            _start_date = datetime.strptime(start_date, "%Y-%m-%d")
            _end_date = datetime.strptime(end_date, "%Y-%m-%d")
        except:
            pass

    postgres = _get_postgres(db_username=db_username, db_password=db_password,
                             db_hostname=db_hostname, db_port=db_port, db_database=db_database, db_schema=db_schema)

    return postgres.tweets_information_by_bbox_and_time(start_date=_start_date, end_date=_end_date, x_min=x_min, y_min=y_min, x_max=x_max, y_max=x_max, tag=tag, lang=language)


def twitter_export_db_to_csv(file_path: str, start_date: datetime, end_date: datetime, x_min, y_min, x_max, y_max, tag='', lang=None, overwrite_file=True,
                             db_username='', db_password='', db_hostname='', db_port='', db_database='', db_schema='',
                             verbose=False):

    postgres = _get_postgres(
        db_username=db_username, db_password=db_password, db_hostname=db_hostname, db_port=db_port, db_database=db_database, db_schema=db_schema)

    if file_path == '' or file_path is None:
        raise ValueError('file_path was not specified.')
    try:
        Folders.make_parent_dir_with_check(file_path=file_path)
    except:
        raise Exception(
            f'Parent folder of the specified file ({file_path}) is not accessible.')

    if Path(file_path).exists():
        if overwrite_file:
            try:
                os.remove(file_path)
            except:
                raise Exception(
                    'The file already exists and it is not possible to delete it.')

    _start_date = None
    _end_date = None

    try:
        _start_date = datetime.strptime(start_date, "%Y-%m-%d")
        _end_date = datetime.strptime(end_date, "%Y-%m-%d")
    except:
        raise ValueError(
            'start_date or end_date were not provided, or they were not in proper formats. The date string should be provided as yyyy-mm-dd.')

    # TODO: The columns arg is neglected. Add it.
    df, num = postgres.read_data_from_postgres(
        start_date=_start_date, end_date=_end_date, x_min=x_min, y_min=y_min, x_max=x_max, y_max=y_max, tag=tag, lang=lang, verbose=verbose)

    if num > 0 and df is not None:
        if verbose:
            print('\tStart writting data to the file ...')
        s_time = datetime.now()
        df.to_csv(file_path)

        dur = datetime.now() - s_time
        if verbose:
            print('\tWriting data was finished ({} seconds).'.format(dur.seconds))
            print(f'File: {file_path}')
    else:
        print(
            'No record in the database satisfied the conditions. No file was created.')


def twitter_import_jsonl_folder_to_db(folder_path: str,
                                      move_imported_to_folder=False,
                                      continue_on_error=True,
                                      start_date: datetime = None,
                                      end_date: datetime = None,
                                      force_insert=True,
                                      bbox_w=0, bbox_e=0, bbox_n=0, bbox_s=0,
                                      tag='',
                                      numb_of_tweets_per_hour_allowed_for_user=.5,
                                      clean_text=False,
                                      db_username='', db_password='', db_hostname='', db_port='', db_database='', db_schema='',
                                      verbose=False):

    if not os.path.exists(folder_path):
        raise ValueError(f"The folder ({folder_path}) does not exist!")

    imported_folder_path = None
    if move_imported_to_folder:
        imported_folder_path = os.path.join(folder_path, 'imported')
        Folders.make_dir_with_check(imported_folder_path)

    number_of_tweets_inserted = 0

    pathlist = Path(folder_path).glob('**/*.json*')
    for path in pathlist:
        path_in_str = str(path)
        number_of_tweets_inserted += twitter_import_jsonl_file_to_db(path_in_str,
                                                                     continue_on_error,
                                                                     start_date, end_date,
                                                                     force_insert,
                                                                     bbox_w, bbox_e, bbox_n, bbox_s,
                                                                     tag,
                                                                     numb_of_tweets_per_hour_allowed_for_user,
                                                                     clean_text,
                                                                     db_username=db_username, db_password=db_password, db_hostname=db_hostname, db_port=db_port, db_database=db_database, db_schema=db_schema)

        if move_imported_to_folder:
            shutil.move(path_in_str, os.path.join(
                imported_folder_path, path.name))

    return number_of_tweets_inserted


def twitter_import_jsonl_file_to_db(file_path: str,
                                    continue_on_error=True,
                                    start_date: datetime = None, end_date: datetime = None,
                                    force_insert=True,
                                    bbox_w=0, bbox_e=0, bbox_n=0, bbox_s=0,
                                    tag='',
                                    numb_of_tweets_per_hour_allowed_for_user=.5,
                                    clean_text=False,
                                    db_username='', db_password='', db_hostname='', db_port='', db_database='', db_schema='',
                                    verbose=False):

    postgres = _get_postgres(
        db_username=db_username, db_password=db_password, db_hostname=db_hostname, db_port=db_port, db_database=db_database, db_schema=db_schema)

    if verbose:
        print(
            f"Import tweets from {os.path.basename(file_path)} to the postgres data.")

    if not os.path.exists(file_path):
        raise ValueError(f"The file ({file_path}) does not exist!")

    postgres = _get_postgres(
        db_username=db_username, db_password=db_password, db_hostname=db_hostname, db_port=db_port, db_database=db_database, db_schema=db_schema)

    number_of_tweets_inserted = 0

    if pathlib.PurePosixPath(file_path).suffix.lower() == '.gz':
        num_lines = sum(1 for line in gzip.open(
            file_path) if (line.strip()) != "")
        with gzip.open(file_path) as f:
            with tqdm(total=num_lines, desc="File \t{}".format(os.path.basename(file_path)), position=0, leave=True) as pbar:
                number_of_tweets_inserted = _worker_jsonl_file_to_db(
                    f, file_path, tag, num_lines, force_insert, clean_text, pbar, postgres)
    else:
        num_lines = sum(1 for line in open(
            file_path) if (line.strip()) != "")
        with open(file_path, 'r', encoding='utf-8') as f:
            with tqdm(total=num_lines, position=0, leave=True) as pbar:
                number_of_tweets_inserted = _worker_jsonl_file_to_db(
                    f, file_path, tag, num_lines, force_insert, clean_text, continue_on_error, pbar, postgres)
    if verbose:
        print(f"\t{number_of_tweets_inserted} tweets imported.")
    return number_of_tweets_inserted


def _worker_jsonl_file_to_db(f, file_path, tag, num_lines, force_insert, clean_text, continue_on_error,
                             pbar, postgres):

    chunks = 100
    number_of_tweets_inserted = 0
    tweet_lines_to_insert = []
    ln = 0

    for line in f:
        if (line.strip()) != "":
            try:
                json.loads(line.strip())
                tweet_lines_to_insert.append(line.strip())
                ln += 1
            except:
                if continue_on_error:
                    print(F'Error in parsing file {file_path}')
                else:
                    raise Exception(
                        'Error in parsing file {}'.format(file_path))

        if len(tweet_lines_to_insert) > 0 and (len(tweet_lines_to_insert) % chunks == 0 or ln == num_lines):
            num = 0
            with suppress_stdout():
                try:
                    num = postgres.bulk_insert_tweets(tweet_lines_to_insert, force_insert=force_insert, clean_text=clean_text,
                                                      tag=tag)
                except:
                    if continue_on_error:
                        print('Error in inserting tweets')
                    else:
                        raise Exception('Error in inserting tweets')

            number_of_tweets_inserted += num
            pbar.update(len(tweet_lines_to_insert))
            tweet_lines_to_insert.clear()
    return number_of_tweets_inserted


def _get_postgres(db_username, db_password, db_hostname, db_port, db_database, db_schema):
    postgres = PostgresHandler_Tweets(
        DB_DATABASE=db_database, DB_HOSTNAME=db_hostname, DB_PORT=db_port, DB_USERNAME=db_username, DB_PASSWORD=db_password, DB_SCHEMA=db_schema)
    postgres.check_db()
    return postgres
