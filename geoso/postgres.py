import json
from copy import copy
from datetime import datetime, timedelta
import sqlalchemy
from sqlalchemy.sql.elements import collate
from sqlalchemy.sql.expression import null
import sqlalchemy_utils
from geoalchemy2 import Geometry
from sqlalchemy import Column
from sqlalchemy import Integer, String, BigInteger, DateTime
from sqlalchemy import MetaData
from sqlalchemy import Table, select, func
from sqlalchemy import create_engine, Numeric, Boolean, ForeignKey, Sequence
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine.url import URL
from sqlalchemy.engine.url import make_url
from sqlalchemy.orm import Session

import pandas as pd
import traceback
import sys


from .clean_text import TextCleaner
from .utils import EnvVar


class PostgresHandler:
    min_acceptable_num_words_in_tweet = 4
    expected_db_version = 2

    def __init__(self, DB_HOSTNAME, DB_PORT, DB_DATABASE, DB_USERNAME, DB_PASSWORD, DB_SCHEMA):

        if DB_HOSTNAME == '' or DB_PORT == '' or DB_DATABASE == '' or DB_USERNAME == '' or DB_HOSTNAME is None or DB_PORT is None or DB_DATABASE is None or DB_USERNAME is None:
            DB_HOSTNAME, DB_PORT, DB_USERNAME, DB_PASSWORD, DB_DATABASE, DB_SCHEMA = EnvVar.get_db_env_variables_if_none(
                DB_HOSTNAME, DB_PORT, DB_USERNAME, DB_PASSWORD, DB_DATABASE, DB_SCHEMA)

        if DB_HOSTNAME == '' or DB_PORT == '' or DB_DATABASE == '' or DB_USERNAME == '':
            raise ValueError(
                'DB_HOSTNAME, DB_PORT, DB_DATABASE, DB_USERNAME or DB_PASSWORD where not provided properly. These variables were not found as Environmental Variables as well.')

        self.postgres_db = {'drivername': 'postgresql',
                            'username': DB_USERNAME,
                            'password': DB_PASSWORD,
                            'host': DB_HOSTNAME,
                            'port': DB_PORT,
                            'database': DB_DATABASE}

        self.db_schema = DB_SCHEMA if DB_SCHEMA != '' and DB_SCHEMA is not None else 'public'

        self.db_url = URL(**self.postgres_db)

        self.db_version = None
        self.engine = None
        self.db_is_checked = False
        self.table_tweet = None
        self.table_twitter_user = None
        self.table_hashtag = None
        self.table_tweet_hashtag = None

    def __del__(self):
        # if self.engine is not None:
        #     self.engine.dispose()
        pass

    def set_db_url(self, postgres_db: dict):
        self.postgres_db = postgres_db
        self.db_url = URL(**self.postgres_db)
        self.db_version = None
        self.engine = None
        self.db_is_checked = False

    def get_db_version(self):
        try:
            check_version_sql = f"SELECT value FROM {self.db_schema}.db_properties WHERE key='version';"
            res = self.engine.execute(check_version_sql)
            for r in res:
                return int(r[0])
        except:
            self.db_version = None
        return self.db_version

    def create_database_schema_version01(self):
        meta = MetaData(self.engine)
        property_table = Table(f'db_properties', meta,
                               Column('key', String, primary_key=True),
                               Column('value', String),
                               )
        property_table.schema = self.db_schema

        meta.create_all()
        property_table_sql_insert_version_1 = f"INSERT INTO {self.db_schema}.db_properties (key, value) " \
                                              "VALUES ('version', '{}');".format(
                                                  "1")
        self.engine.execute(property_table_sql_insert_version_1)
        pass

    def create_database_schema_version02(self):
        meta = MetaData(self.engine)

        user_table = Table(f'twitter_user', meta,
                           Column('id', BigInteger, primary_key=True),
                           Column('name', String(100), nullable=True),
                           Column('screen_name', String(100), nullable=True),
                           Column('location', String(1000), nullable=True),
                           Column('followers_count', Integer),
                           Column('friends_count', Integer),
                           Column('listed_count', Integer),
                           Column('favourites_count', Integer),
                           Column('statuses_count', Integer),
                           Column('geo_enabled', Boolean),
                           Column('lang', String(5), nullable=True),
                           )
        user_table.schema = self.db_schema

        hashtag_table = Table(f'hashtag', meta,
                              Column('id', Integer, primary_key=True,
                                     autoincrement=True),
                              Column('value', String(100), unique=True),
                              )
        hashtag_table.schema = self.db_schema

        tweet_table = Table(f'tweet', meta,
                            Column('id', BigInteger, primary_key=True),
                            Column('num', BigInteger, autoincrement=True),
                            Column('t_datetime', DateTime),
                            Column('t', BigInteger),
                            Column('created_at', DateTime),
                            Column('lang', String(5), nullable=True),
                            Column('user_id', BigInteger,
                                   ForeignKey('twitter_user.id')),
                            Column('user_screen_name',
                                   String(100), nullable=True),
                            Column('country', String(100), nullable=True),
                            Column('country_code', String(5), nullable=True),
                            Column('x', Numeric, nullable=True),
                            Column('y', Numeric, nullable=True),
                            Column('text', String(1000)),
                            Column('c', String(1000)),
                            Column('geom4326', Geometry(
                                'POINT', srid=4326), nullable=True),
                            Column('in_reply_to_status_id',
                                   BigInteger, nullable=True),
                            Column('in_reply_to_user_id',
                                   BigInteger, nullable=True),
                            Column('in_reply_to_screen_name',
                                   String(100), nullable=True),
                            Column('quoted_status_id',
                                   BigInteger, nullable=True),
                            Column('is_quote_status', Boolean, nullable=True),
                            Column('quote_count', Integer, nullable=True),
                            Column('reply_count', Integer, nullable=True),
                            Column('retweet_count', Integer, nullable=True),
                            Column('favorited', Boolean, nullable=True),
                            Column('retweeted', Boolean, nullable=True),
                            Column('Tag', String(100), nullable=True),
                            Column('lang_supported', Boolean, nullable=True),
                            Column('hashtags_', String(1000), nullable=True),
                            Column('tweet_json', sqlalchemy.JSON),
                            )
        tweet_table.schema = self.db_schema

        hashtag_tweet_table = Table(f'tweet_hashtag', meta,
                                    Column('tweet_id', BigInteger, ForeignKey(
                                        'tweet.id'), primary_key=True),
                                    Column('hashtag_id', Integer, ForeignKey(
                                        'hashtag.id'), primary_key=True),
                                    )
        hashtag_tweet_table.schema = self.db_schema

        meta.create_all()
        property_table_sql_insert_version_2 = "UPDATE {}.db_properties SET value = '{}' WHERE key ='version'; ".format(self.db_schema,
                                                                                                                       "2")
        self.engine.execute(property_table_sql_insert_version_2)

        pass

    def load_schema(self):
        meta = MetaData(self.engine, schema=self.db_schema)
        self.table_tweet = Table(
            f'tweet', meta, autoload=True, autoload_with=self.engine)
        self.table_twitter_user = Table(f'twitter_user', meta, autoload=True,
                                        autoload_with=self.engine)
        self.table_hashtag = Table(f'hashtag', meta, autoload=True,
                                   autoload_with=self.engine)
        self.table_tweet_hashtag = Table(f'tweet_hashtag', meta, autoload=True,
                                         autoload_with=self.engine)

    def db_exists(self):
        try:
            # print(self.db_url)
            t = sqlalchemy_utils.database_exists(self.db_url)
            return t
        except:
            res = self.engine.execute(
                f"select exists(SELECT datname FROM pg_catalog.pg_database WHERE lower(datname) = lower('{self.db_url.database}'));")
            for row in res:
                return row[0]

    def db_schema_exists(self):
        res = self.engine.execute(
            f"SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspowner <> 1 AND nspname = '{self.db_schema}');")
        for row in res:
            return row[0]

    def check_db(self):
        if not self.db_is_checked:

            self.engine = create_engine(
                self.db_url, isolation_level="AUTOCOMMIT", pool_size=10, max_overflow=20)

            if not self.db_exists():
                sqlalchemy_utils.create_database(self.db_url)

            if self.db_schema != 'public':
                self.engine.execute(
                    f"CREATE SCHEMA IF NOT EXISTS {self.db_schema};")
                self.engine.execute(
                    f"SET search_path TO {self.db_schema}, public;")

            # Postgis extension needs to be installed in public
            self.engine.execute(
                f"CREATE EXTENSION IF NOT EXISTS postgis SCHEMA public;")
            self.engine.execute(
                f"CREATE EXTENSION IF NOT EXISTS postgis_topology;")

            db_version = self.get_db_version()
            if db_version is None:
                self.create_database_schema_version01()
                db_version = self.get_db_version()
            if db_version == 1:
                self.create_database_schema_version02()
                db_version = self.get_db_version()

            self.load_schema()
            self.db_version = db_version
            self.db_is_checked = db_version is not None
            if self.db_version != PostgresHandler.expected_db_version:
                raise Exception("Current db version ({}) differs from expected db version ({})".format(self.db_version,
                                                                                                       PostgresHandler.expected_db_version))

        return self.db_is_checked


class PostgresHandler_Tweets(PostgresHandler):

    def __init__(self, DB_HOSTNAME, DB_PORT, DB_DATABASE, DB_USERNAME, DB_PASSWORD,  DB_SCHEMA):
        super().__init__(DB_HOSTNAME, DB_PORT, DB_DATABASE,
                         DB_USERNAME, DB_PASSWORD, DB_SCHEMA)

    def delete_old_tweets(self, num_days=90):
        sql = F"DELETE FROM {self.db_schema}.tweet_hashtag WHERE tweet_id in (SELECT id from {self.db_schema}.tweet WHERE EXTRACT(DAY FROM NOW() - created_at) > {num_days});"
        self.engine.execute(sql)
        sql = F"DELETE FROM {self.db_schema}.tweet WHERE EXTRACT(DAY FROM NOW() - created_at) > {num_days};"
        self.engine.execute(sql)
        pass

    def read_data_from_postgres(self, start_date: datetime, end_date: datetime, x_min, y_min, x_max, y_max, table_name='tweet', tag='', lang=None,
                                columns: list = ['id', 'x', 'y', 't', 'text'], verbose=False):
        # todo: check if the table exists and catch any error
        if verbose:
            print('\tStart reading data ...')
        s_time = datetime.now()

        if x_min is None or y_min is None or x_max is None or y_max is None or start_date is None or end_date is None:
            raise ValueError(
                "Either date arguments (start_date and end_date) or bounding box arguments (x_min, y_min, x_max, and y_max) are not provided properly.")

        start = datetime(year=start_date.year, month=start_date.month,
                         day=start_date.day, hour=start_date.hour, minute=start_date.minute)
        end = datetime(year=end_date.year, month=end_date.month,
                       day=end_date.day, hour=end_date.hour, minute=end_date.minute)

        col = ','.join(columns) if columns is not None and len(
            columns) > 0 else '*'
        sql = self._get_select_bbox_time(col, tag, lang)

        self.check_db()

        tweets = pd.read_sql_query(
            sql, self.engine, params=(start, end, x_min, x_max, y_min, y_max))

        if 't' in columns:
            tweets['t_datetime'] = tweets['t'].apply(
                pd.Timestamp.fromtimestamp)

        number_of_tweets = tweets.id.count()

        dur = datetime.now() - s_time
        if verbose:
            print('\tReading data was finished ({} seconds).'.format(dur.seconds))
        return tweets, number_of_tweets

    def extract_hashtags(self, tweet_json):
        hashtags = []
        for d in tweet_json['entities']['hashtags']:
            hashtags.append(d['text'][0:99])
        return hashtags
        pass

    def value_or_none(self, dic: dict, key: str):
        if key in dic:
            return dic[key]
        else:
            return None

    def bulk_insert_tweets(self, tweets: list, country_code: str = '', tag='',
                           only_geotagged=False, bbox_w=None, bbox_e=None, bbox_n=None, bbox_s=None,
                           force_insert=False, clean_text=False):
        self.check_db()
        num_inserted_tweets = 0
        lst_users = []
        lst_tweets = []
        lst_tweet_ids = []
        lst_hashtags = []
        lst_tweet_hashtags = []
        for t in tweets:
            tweet_json = json.loads(t)
            x = None
            y = None
            add_it = True
            if tweet_json['coordinates'] is not None or tweet_json['geo'] is not None:
                # source: https://developer.twitter.com/en/docs/tutorials/filtering-tweets-by-location
                if tweet_json['coordinates'] is not None:
                    x = float(tweet_json['coordinates']['coordinates'][0])
                    y = float(tweet_json['coordinates']['coordinates'][1])
                else:
                    x = float(tweet_json['geo']['coordinates'][1])
                    y = float(tweet_json['geo']['coordinates'][0])
            if only_geotagged:
                if x is None or y is None:
                    add_it = False

                if bbox_e is not None and bbox_n is not None and bbox_s is not None and bbox_w is not None:
                    if x is not None and y is not None:
                        if not (x >= bbox_w and x <= bbox_e and y <= bbox_n and y >= bbox_s):
                            add_it = False

            if country_code != '':
                try:
                    if country_code != tweet_json['place']['country_code']:
                        add_it = False
                except:
                    add_it = False

            cleaned_text = ''
            lang_supported = False
            num_of_words = 0
            if TextCleaner.is_lang_supported(tweet_json['lang']):
                if clean_text:
                    cleaned_text, num_of_words, lang_full_name = TextCleaner.clean_text(tweet_json['text'],
                                                                                        tweet_json['lang'])
                else:
                    cleaned_text = ''
                    num_of_words = len(str(tweet_json['text']).split())
                lang_supported = True
            else:
                cleaned_text = ''
                num_of_words = len(str(tweet_json['text']).split())
                lang_supported = False

            if num_of_words < PostgresHandler.min_acceptable_num_words_in_tweet:
                add_it = False

            if add_it:
                self._add_tweet_to_insert_list(tweet_json["text"], cleaned_text, lang_supported, lst_hashtags,
                                               lst_tweet_hashtags,
                                               lst_tweet_ids, lst_tweets, lst_users, tag, tweet_json, x, y)

        if force_insert:
            if len(lst_tweet_ids) > 0:
                self.engine.execute(
                    f"DELETE FROM {self.db_schema}.tweet_hashtag WHERE tweet_hashtag.tweet_id in ({','.join(str(x) for x in lst_tweet_ids)});")
                self.engine.execute(
                    f"DELETE FROM {self.db_schema}.tweet WHERE tweet.id in ({','.join(str(x) for x in lst_tweet_ids)});")
        if len(lst_tweets) > 0:
            if len(lst_users) > 0:
                self.engine.execute(pg_insert(self.table_twitter_user).on_conflict_do_nothing(index_elements=['id']),
                                    lst_users)
            if len(lst_tweets) > 0:
                res = self.engine.execute(pg_insert(self.table_tweet).on_conflict_do_nothing(index_elements=['id']),
                                          lst_tweets)
                num_inserted_tweets = res.rowcount

            if len(lst_hashtags) > 0:
                self.engine.execute(pg_insert(self.table_hashtag).on_conflict_do_nothing(index_elements=['value']),
                                    lst_hashtags)
            if len(lst_tweet_hashtags) > 0:
                self.engine.execute(f"INSERT INTO {self.db_schema}.tweet_hashtag(tweet_id, hashtag_id) "
                                    "VALUES("
                                    "   %(tweet_id)s, "
                                    f"   (SELECT hashtag.id FROM {self.db_schema}.hashtag WHERE hashtag.value = %(value)s)"
                                    ") ON CONFLICT (tweet_id, hashtag_id) DO NOTHING;",
                                    lst_tweet_hashtags)
        return num_inserted_tweets

    # def bulk_insert_tweets(self, tweets: list, tag='', force_insert=False):
    #     self.check_db()
    #     lst_users = []
    #     lst_tweets = []
    #     lst_tweet_ids = []
    #     lst_hashtags = []
    #     lst_tweet_hashtags = []
    #     for t in tweets:
    #         tweet_json = json.loads(t)
    #         x = None
    #         y = None
    #         add_it = True
    #         if tweet_json['coordinates'] is not None or tweet_json['geo'] is not None:
    #             # source: https://developer.twitter.com/en/docs/tutorials/filtering-tweets-by-location
    #             if tweet_json['coordinates'] is not None:
    #                 x = float(tweet_json['coordinates']['coordinates'][0])
    #                 y = float(tweet_json['coordinates']['coordinates'][1])
    #             else:
    #                 x = float(tweet_json['geo']['coordinates'][1])
    #                 y = float(tweet_json['geo']['coordinates'][0])
    #         # if x is None or y is None:
    #         #     add_it = False

    #         cleaned_text = ''
    #         lang_supported = False
    #         num_of_words = 0
    #         _text = ''
    #         if 'text' in tweet_json:
    #             _text = tweet_json['text']
    #         elif 'full_text' in tweet_json:
    #             _text = tweet_json['full_text']
    #         else:
    #             add_it = False

    #         if add_it:
    #             if tweet_json['lang'] is not None and TextCleaner.is_lang_supported(tweet_json['lang']):
    #                 cleaned_text, num_of_words, lang_full_name = TextCleaner.clean_text(_text,
    #                                                                                     tweet_json['lang'])
    #                 lang_supported = True
    #             else:
    #                 cleaned_text = ''
    #                 num_of_words = len(str(_text).split())
    #                 lang_supported = False

    #         if num_of_words < PostgresHandler.min_acceptable_num_words_in_tweet:
    #             add_it = False

    #         if add_it:
    #             self._add_tweet_to_insert_list(_text, cleaned_text, lang_supported, lst_hashtags, lst_tweet_hashtags,
    #                                            lst_tweet_ids, lst_tweets, lst_users, tag, tweet_json, x, y)

    #     if force_insert:
    #         if len(lst_tweet_ids) > 0:
    #             with self.engine.begin():
    #                 self.engine.execute(
    #                     f"DELETE FROM {self.db_schema}.tweet WHERE tweet.id in ({','.join(str(x) for x in lst_tweet_ids)});")

    #     if len(lst_tweets) > 0:
    #         with self.engine.begin():
    #             if len(lst_users) > 0:
    #                 self.engine.execute(
    #                     pg_insert(self.table_twitter_user).on_conflict_do_nothing(
    #                         index_elements=['id']),
    #                     lst_users)
    #             if len(lst_tweets) > 0:
    #                 self.engine.execute(pg_insert(self.table_tweet).on_conflict_do_nothing(index_elements=['id']),
    #                                     lst_tweets)
    #             if len(lst_hashtags) > 0:
    #                 self.engine.execute(pg_insert(self.table_hashtag).on_conflict_do_nothing(index_elements=['value']),
    #                                     lst_hashtags)
    #             if len(lst_tweet_hashtags) > 0:
    #                 self.engine.execute(f"INSERT INTO {self.db_schema}.tweet_hashtag(tweet_id, hashtag_id) "
    #                                     "VALUES("
    #                                     "   %(tweet_id)s, "
    #                                     f"   (SELECT hashtag.id FROM {self.db_schema}.hashtag WHERE hashtag.value = %(value)s)"
    #                                     ") ON CONFLICT (tweet_id, hashtag_id) DO NOTHING;",
    #                                     lst_tweet_hashtags)
    #     return len(lst_tweets)

    def _add_tweet_to_insert_list(self, _text, cleaned_text, lang_supported, lst_hashtags, lst_tweet_hashtags,
                                  lst_tweet_ids, lst_tweets, lst_users, tag, tweet_json, x, y):
        hashtags = self.extract_hashtags(tweet_json)
        lst_tweet_ids.append(tweet_json["id"])
        lst_users.append({
            "id": tweet_json['user']['id'],
            "name": tweet_json['user']['name'],
            "screen_name": tweet_json['user']['screen_name'],
            "location": str(tweet_json['user']['location'])[0:1000],
            "followers_count": tweet_json['user']['followers_count'],
            "friends_count": tweet_json['user']['friends_count'],
            "listed_count": tweet_json['user']['listed_count'],
            "favourites_count": tweet_json['user']['favourites_count'],
            "statuses_count": tweet_json['user']['statuses_count'],
            "geo_enabled": tweet_json['user']['geo_enabled'],
            "lang": tweet_json['user']['lang'] if tweet_json['user']['lang'] is not None else ''})
        lst_tweets.append({
            "tag": tag,
            "tweet_json": str(tweet_json),
            "lang_supported": lang_supported,
            "hashtags_": " ".join(hashtags) if len(hashtags) > 0 else '',
            "id": tweet_json["id"],
            "text": _text[0:1000],
            "created_at": tweet_json['created_at'],
            "lang": tweet_json['lang'],
            "user_id": tweet_json['user']['id'],
            "user_screen_name": tweet_json['user']['screen_name'],
            "in_reply_to_status_id": tweet_json['in_reply_to_status_id'],
            'in_reply_to_user_id': tweet_json['in_reply_to_user_id'],
            "in_reply_to_screen_name": tweet_json['in_reply_to_screen_name'],
            # "quoted_status_id":tweet_json['quoted_status_id'],
            "is_quote_status": tweet_json['is_quote_status'],
            "quote_count": self.value_or_none(tweet_json, 'quote_count'),
            "reply_count": self.value_or_none(tweet_json, 'reply_count'),
            "retweet_count": self.value_or_none(tweet_json, 'retweet_count'),
            "favorited": self.value_or_none(tweet_json, 'favorited'),
            "retweeted": self.value_or_none(tweet_json, 'retweeted'),
            "country": tweet_json['place']['country'][0:100] if tweet_json['place'] is not None and
            tweet_json['place'][
                'country'] is not None else '',
            "country_code": tweet_json['place']['country_code'] if tweet_json['place'] is not None and
            tweet_json['place'][
                'country_code'] is not None else '',
            "c": cleaned_text[0:1000],
            "t": datetime.strptime(tweet_json['created_at'], '%a %b %d %H:%M:%S %z %Y').timestamp(),
            "t_datetime": datetime.strptime(tweet_json['created_at'], '%a %b %d %H:%M:%S %z %Y'),
            "x": x,
            "y": y})
        [lst_hashtags.append({'value': h})
         for h in hashtags if h.strip() != ""]
        [lst_tweet_hashtags.append({'tweet_id': tweet_json["id"], 'value': h}) for h in hashtags if
         h.strip() != ""]

    def upsert_tweet(self, tweet_text: str, country_code: str = '', bbox_w=0, bbox_e=0, bbox_n=0,
                     bbox_s=0, tag='', force_insert=False) -> bool:
        self.check_db()
        tweet_json = json.loads(tweet_text)
        x = None
        y = None

        if tweet_json['coordinates'] is not None or tweet_json['geo'] is not None:
            # source: https://developer.twitter.com/en/docs/tutorials/filtering-tweets-by-location
            if tweet_json['coordinates'] is not None:
                x = float(tweet_json['coordinates']['coordinates'][0])
                y = float(tweet_json['coordinates']['coordinates'][1])
            else:
                x = float(tweet_json['geo']['coordinates'][1])
                y = float(tweet_json['geo']['coordinates'][0])
        if x is None or y is None:
            return False

        if bbox_e != 0 and bbox_n != 0 and bbox_s != 0 and bbox_w != 0:
            if not (x >= bbox_w and x <= bbox_e and y <= bbox_n and y >= bbox_s):
                return False
        elif country_code != '':
            try:
                if country_code != tweet_json['place']['country_code']:
                    return False
            except:
                return False

        # upsert: https://docs.sqlalchemy.org/en/13/dialects/postgresql.html
        cleaned_text = ''
        lang_supported = False
        num_of_words = 0
        if TextCleaner.is_lang_supported(tweet_json['lang']):
            cleaned_text, num_of_words, lang_full_name = TextCleaner.clean_text(
                tweet_json['text'], tweet_json['lang'])
            lang_supported = True
        else:
            cleaned_text = ''
            num_of_words = len(str(tweet_json['text']).split())
            lang_supported = False

        hashtags = self.extract_hashtags(tweet_json)

        if num_of_words >= PostgresHandler.min_acceptable_num_words_in_tweet:
            ins_user = pg_insert(self.table_twitter_user).values(
                id=tweet_json['user']['id'],
                name=tweet_json['user']['name'],
                screen_name=tweet_json['user']['screen_name'][0:100],
                location=str(tweet_json['user']['location'])[0:1000],
                followers_count=tweet_json['user']['followers_count'],
                friends_count=tweet_json['user']['friends_count'],
                listed_count=tweet_json['user']['listed_count'],
                favourites_count=tweet_json['user']['favourites_count'],
                statuses_count=tweet_json['user']['statuses_count'],
                geo_enabled=tweet_json['user']['geo_enabled'],
                lang=tweet_json['user']['lang'],
            ).on_conflict_do_nothing(index_elements=['id'])
            res_user = self.engine.execute(ins_user)

            ins = pg_insert(self.table_tweet).values(
                tag=tag,
                tweet_json=str(tweet_json),
                lang_supported=lang_supported,
                hashtags_=" ".join(hashtags) if len(hashtags) > 0 else '',
                id=tweet_json["id"],
                text=tweet_json["text"],
                created_at=tweet_json['created_at'],
                lang=tweet_json['lang'],
                user_id=tweet_json['user']['id'],
                user_screen_name=tweet_json['user']['screen_name'][0:100],
                in_reply_to_status_id=tweet_json['in_reply_to_status_id'],
                in_reply_to_user_id=tweet_json['in_reply_to_user_id'],
                in_reply_to_screen_name=tweet_json['in_reply_to_screen_name'][0:100],
                # quoted_status_id=tweet_json['quoted_status_id'],
                is_quote_status=tweet_json['is_quote_status'],
                quote_count=self.value_or_none(tweet_json, 'quote_count'),
                reply_count=self.value_or_none(tweet_json, 'reply_count'),
                retweet_count=self.value_or_none(tweet_json, 'retweet_count'),
                favorited=self.value_or_none(tweet_json, 'favorited'),
                retweeted=self.value_or_none(tweet_json, 'retweeted'),
                country=tweet_json['place']['country'][0:100] if tweet_json['place'] is not None and tweet_json['place'][
                    'country'] is not None else '',
                country_code=tweet_json['place']['country_code'] if tweet_json['place'] is not None and
                tweet_json['place'][
                    'country_code'] is not None else '',
                c=cleaned_text,
                t=datetime.strptime(
                    tweet_json['created_at'], '%a %b %d %H:%M:%S %z %Y').timestamp(),
                t_datetime=datetime.strptime(
                    tweet_json['created_at'], '%a %b %d %H:%M:%S %z %Y'),
                x=x,
                y=y
            )
            if force_insert:
                ins = ins.on_conflict_do_update(
                    index_elements=['id'],
                    set_=dict(
                        lang_supported=lang_supported,
                        hashtags_=" ".join(hashtags) if len(
                            hashtags) > 0 else '',
                        created_at=tweet_json['created_at'],
                        lang=tweet_json['lang'],
                        tweet_json=str(tweet_json),
                        user_id=tweet_json['user']['id'],
                        user_screen_name=tweet_json['user']['screen_name'][0:100],
                        text=tweet_json['text'],
                        in_reply_to_status_id=tweet_json['in_reply_to_status_id'],
                        in_reply_to_user_id=tweet_json['in_reply_to_user_id'],
                        in_reply_to_screen_name=tweet_json['in_reply_to_screen_name'][0:100],
                        # quoted_status_id=tweet_json['quoted_status_id'],
                        is_quote_status=tweet_json['is_quote_status'],
                        quote_count=self.value_or_none(
                            tweet_json, 'quote_count'),
                        reply_count=tweet_json['reply_count'],
                        retweet_count=tweet_json['retweet_count'],
                        favorited=tweet_json['favorited'],
                        retweeted=tweet_json['retweeted'],
                        country=tweet_json['place']['country'][0:100] if tweet_json['place'] is not None and
                        tweet_json['place'][
                            'country'] is not None else '',
                        country_code=tweet_json['place']['country_code'] if tweet_json['place'] is not None and
                        tweet_json['place'][
                            'country_code'] is not None else '',
                        c=cleaned_text,
                        t=datetime.strptime(
                            tweet_json['created_at'], '%a %b %d %H:%M:%S %z %Y').timestamp(),
                        t_datetime=datetime.strptime(
                            tweet_json['created_at'], '%a %b %d %H:%M:%S %z %Y'),
                        x=x,
                        y=y
                    )
                )
            else:
                ins = ins.on_conflict_do_nothing(index_elements=['id'])
            res = self.engine.execute(ins)

            session = Session()
            for h in hashtags:
                ins_hashtag = pg_insert(self.table_hashtag).values(
                    value=h
                ).on_conflict_do_nothing(index_elements=['value'])
                res_hashtag = self.engine.execute(ins_hashtag)
                hashtag_id = None
                if res_hashtag.rowcount > 0:
                    hashtag_id = res_hashtag.inserted_primary_key[0]
                else:
                    hashtag_id = session.query(
                        self.table_hashtag).filter_by(value=h).first()[0]
                # print("Hashtag id: {}".format(hashtag_id ))
                ins_tweet_hashtag = pg_insert(self.table_tweet_hashtag).values(
                    tweet_id=tweet_json["id"],
                    hashtag_id=hashtag_id
                ).on_conflict_do_nothing(index_elements=['tweet_id', 'hashtag_id'])
                res_tweet_hashtag = self.engine.execute(ins_tweet_hashtag)
                # print("Tweet_Hashtag id: {}".format(res_tweet_hashtag.inserted_primary_key[0]))
            return res.rowcount > 0
        return False

    def number_of_tweets(self):
        self.check_db()
        return self.engine.execute(F'SELECT count(*) FROM {self.db_schema}.tweet;').scalar()

    def tweets_information_by_bbox_and_time(self,
                                            start_date: datetime = None, end_date: datetime = None,
                                            x_min: float = None, y_min: float = None, x_max: float = None, y_max: float = None,
                                            tag=None, lang=None) -> pd.DataFrame:

        col = ' COUNT(*) AS num_tweets, MIN(t_datetime) AS min_date, MAX(t_datetime) AS max_date, MIN(x) AS x_min, MIN(y) as y_min, MAX(x) AS x_max, MAX(y) as y_max '

        flag_bbox = x_min is not None and y_min is not None and x_max is not None and y_min is not None
        flag_time = start_date is not None and end_date is not None

        results = None

        if flag_bbox and flag_time:
            return pd.read_sql_query(
                self._get_select_bbox_time(col=col, tag=tag, lang=lang),
                self.engine, params=(start_date, end_date, x_min, x_max, y_min, y_max))
        elif flag_time:
            return pd.read_sql_query(
                self._get_select_time(col=col, tag=tag, lang=lang),
                self.engine, params=(start_date, end_date))
        elif flag_bbox:
            return pd.read_sql_query(
                self._get_select_bbox(col=col, tag=tag, lang=lang),
                self.engine, params=(x_min, x_max, y_min, y_max))
        else:
            return pd.read_sql_query(F'SELECT {col} FROM {self.db_schema}.tweet;',
                                     self.engine)

        return results

    def _get_select_time(self, col: str, tag, lang):
        sql = F" SELECT {col} FROM  {self.db_schema}.tweet " \
            " WHERE " \
            " t_datetime > %s AND " \
            " t_datetime <= %s "

        if tag is not None and tag != '':
            sql = sql + " AND tag=\'{}\'".format(tag)

        if lang is not None and lang != '':
            sql = sql + F" AND lang=\'{lang}\' "
        return sql

    def _get_select_bbox_time(self, col: str, tag, lang):
        sql = F" SELECT {col} FROM  {self.db_schema}.tweet " \
            " WHERE " \
            " t_datetime > %s AND " \
            " t_datetime <= %s AND " \
            " x >= %s AND x < %s AND" \
            " y >= %s AND y < %s "

        if tag is not None and tag != '':
            sql = sql + " AND tag=\'{}\'".format(tag)

        if lang is not None and lang != '':
            sql = sql + F" AND lang=\'{lang}\' "
        return sql

    def _get_select_bbox(self, col: str, tag, lang):
        sql = F" SELECT {col} FROM  {self.db_schema}.tweet " \
            " WHERE " \
            " x >= %s AND x < %s AND" \
            " y >= %s AND y < %s "

        if tag is not None and tag != '':
            sql = sql + " AND tag=\'{}\'".format(tag)

        if lang is not None and lang != '':
            sql = sql + F" AND lang=\'{lang}\' "
        return sql

    def update_geom(self):
        updateQuery = "update public.tweet set geom4326=ST_SetSRID(ST_MakePoint(x, y), 4326);"
        self.engine.execute(updateQuery)
