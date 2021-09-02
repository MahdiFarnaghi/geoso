from abc import abstractclassmethod, abstractmethod
import os
from datetime import datetime

from tqdm import tqdm
from pathlib import Path

from email.utils import mktime_tz, parsedate_tz
from datetime import datetime
import pytz

from .utils import suppress_stdout
from .postgres import PostgresHandler_Tweets
import abc
import gzip
import pathlib
import json


class TweetReaderWriter:

    postgres = None

    @abc.abstractmethod
    def jsonl_folder_to_postgres(folder_path: str,
                                 start_date: datetime = None,
                                 end_date: datetime = None,
                                 force_insert=True,
                                 bbox_w=0, bbox_e=0, bbox_n=0, bbox_s=0,
                                 tag='',
                                 numb_of_tweets_per_hour_allowed_for_user=.5,
                                 clean_text=False,
                                 db_username='', db_password='', db_hostname='', db_port='', db_database='', db_schema=''):

        if not os.path.exists(folder_path):
            raise ValueError(f"The folder ({folder_path}) does not exist!")

        TweetReaderWriter.check_postgres(
            db_username=db_username, db_password=db_password, db_hostname=db_hostname, db_port=db_port, db_database=db_database, db_schema=db_schema)

        number_of_tweets_inserted = 0

        pathlist = Path(folder_path).glob('**/*.json*')
        for path in pathlist:
            path_in_str = str(path)
            number_of_tweets_inserted += TweetReaderWriter.jsonl_file_to_postgres(path_in_str, start_date, end_date,
                                                                                  force_insert,
                                                                                  bbox_w, bbox_e, bbox_n, bbox_s,
                                                                                  tag,
                                                                                  numb_of_tweets_per_hour_allowed_for_user,
                                                                                  clean_text)

        return number_of_tweets_inserted

    @abc.abstractmethod
    def jsonl_file_to_postgres(file_path: str,
                               start_date: datetime = None, end_date: datetime = None,
                               force_insert=True,
                               bbox_w=0, bbox_e=0, bbox_n=0, bbox_s=0,
                               tag='',
                               numb_of_tweets_per_hour_allowed_for_user=.5,
                               clean_text=False,
                               db_username='', db_password='', db_hostname='', db_port='', db_database='', db_schema=''):

        print(
            f"Import tweets from {os.path.basename(file_path)} to the postgres data.")

        if not os.path.exists(file_path):
            raise ValueError(f"The file ({file_path}) does not exist!")

        TweetReaderWriter.check_postgres(
            db_username=db_username, db_password=db_password, db_hostname=db_hostname, db_port=db_port, db_database=db_database, db_schema=db_schema)

        number_of_tweets_inserted = 0

        if pathlib.PurePosixPath(file_path).suffix.lower() == '.gz':
            num_lines = sum(1 for line in gzip.open(
                file_path) if (line.strip()) != "")
            with gzip.open(file_path) as f:
                with tqdm(total=num_lines, desc="File \t{}".format(os.path.basename(file_path)), position=0, leave=True) as pbar:
                    number_of_tweets_inserted = TweetReaderWriter._jsonl_file_to_postgres(
                        f, file_path, tag, num_lines, force_insert, clean_text, pbar, TweetReaderWriter.postgres)
        else:
            num_lines = sum(1 for line in open(
                file_path) if (line.strip()) != "")
            with open(file_path, 'r', encoding='utf-8') as f:
                with tqdm(total=num_lines, position=0, leave=True) as pbar:
                    number_of_tweets_inserted = TweetReaderWriter._jsonl_file_to_postgres(
                        f, file_path, tag, num_lines, force_insert, clean_text, pbar, TweetReaderWriter.postgres)
        print(f"\t{number_of_tweets_inserted} tweets imported.")
        return number_of_tweets_inserted

    @staticmethod
    def _jsonl_file_to_postgres(f, file_path, tag, num_lines, force_insert, clean_text, pbar, postgres):

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
                    raise Exception(
                        'Error in parsing file {}'.format(file_path))

            if len(tweet_lines_to_insert) > 0 and (len(tweet_lines_to_insert) % chunks == 0 or ln == num_lines):
                num = 0
                with suppress_stdout():
                    num = postgres.bulk_insert_geotagged_tweets(tweet_lines_to_insert, force_insert=force_insert, clean_text=clean_text,
                                                                tag=tag)
                number_of_tweets_inserted += num
                pbar.update(len(tweet_lines_to_insert))
                tweet_lines_to_insert.clear()
        return number_of_tweets_inserted

    @abstractmethod
    def check_postgres(db_username, db_password, db_hostname, db_port, db_database, db_schema):
        if TweetReaderWriter.postgres is None:
            TweetReaderWriter.postgres = PostgresHandler_Tweets(
                DB_DATABASE=db_database, DB_HOSTNAME=db_hostname, DB_PORT=db_port, DB_USERNAME=db_username, DB_PASSWORD=db_password, DB_SCHEMA=db_schema)
            TweetReaderWriter.postgres.check_db()

    @abstractclassmethod
    def _parse_datetime_to_system_local_time(value):
        time_tuple = parsedate_tz(value)
        timestamp = mktime_tz(time_tuple)
        return datetime.fromtimestamp(timestamp)

    @abstractclassmethod
    def _parse_datetime_to_timezone(value, time_zone):
        return datetime.strptime(value, '%a %b %d %H:%M:%S %z %Y').astimezone(pytz.timezone(time_zone))
