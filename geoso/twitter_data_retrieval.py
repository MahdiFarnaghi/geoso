from .postgres import PostgresHandler_Tweets
from .utils import Folders, Text_Categories, dprint, EnvVar
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import datetime
import os
import json
import traceback
import sys
from dotenv import load_dotenv
import time
from urllib3.exceptions import ProtocolError


def twitter_retrieve_data_streaming_api(consumer_key=None, consumer_secret=None, access_token=None, access_secret=None, save_data_mode=None,
                                        tweets_output_folder=None, area_name='',  x_min: float=None, x_max: float=None, y_min: float=None, y_max: float=None,
                                        languages=None, max_num_tweets: int=None, only_geotagged=True, db_hostname=None, db_port=None, db_database=None, db_schema='Public', db_username=None, db_password=None, verbose=True):

    dprint('Start: twitter_retrieve_data_streaming_api process', verbose=verbose,
           text_category=Text_Categories.Process_start)

    load_dotenv()

    if save_data_mode is None:
        save_data_mode = os.getenv('SAVE_DATA_MODE')
        if save_data_mode is None:
            save_data_mode = 'FILE'
    else:
        save_data_mode = save_data_mode.upper()
        if not(save_data_mode.upper() == 'FILE' or save_data_mode.upper() == 'DB'):
            raise ValueError('save_data_mode can be either FILE or DB.')

    dprint(f"Save data mode: {save_data_mode}", verbose=verbose)

    if consumer_key is None or consumer_secret is None or access_token is None or access_secret is None:
        consumer_key, consumer_secret, access_token, access_secret = EnvVar.get_twitter_credentials_env_variables()

    if consumer_key is None or consumer_secret is None or access_token is None or access_secret is None:
        raise Exception(
            "Twitter authentication information (consumer_key, consumer_secret, access_token, and access_secret) was not provided properly.")

    dprint('Authentication ...', verbose=verbose)
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    api = tweepy.API(auth)
    dprint('Authentication was successful.', verbose=verbose)

    if x_min is None or y_min is None or x_max is None or y_max is None:
        x_min = float(os.getenv('x_min')) if os.getenv(
            'x_min') is not None else None
        x_max = float(os.getenv('x_max')) if os.getenv(
            'x_max') is not None else None
        y_min = float(os.getenv('y_min')) if os.getenv(
            'y_min') is not None else None
        y_max = float(os.getenv('y_max')) if os.getenv(
            'y_max') is not None else None

    dprint(
        f"BBOX ({area_name}): {x_min}, {y_min}, {x_max}, {y_max}", verbose=verbose)

    if tweets_output_folder is None:
        tweets_output_folder = os.getenv('TWEETS_OUTPUT_FOLDER')

    if save_data_mode == 'FILE':
        if tweets_output_folder is None:
            raise Exception(
                "tweets_output_folder was not provided.")
        else:
            try:
                Folders.make_dir_with_check(tweets_output_folder)
            except:
                raise Exception(
                    "The specified tweets_output_folder {tweets_output_folder} is not accessible.")

    if area_name == '':
        area_name = os.getenv('AREA_NAME') if os.getenv(
            'AREA_NAME') is not None else ''

    if languages is None:
        languages = os.getenv('LANGUAGES')
        dprint(f"Languages: {', '.join(languages)}", verbose=verbose)
    if languages is not None and languages != '':
        languages = str(languages).split(',')
        languages = [str.strip(lang) for lang in languages]

    if save_data_mode == 'FILE':
        _listen_to_tweets(area_name, tweets_output_folder, None,
                          save_data_mode, auth, languages, max_num_tweets, only_geotagged, x_min, y_min, x_max, y_max, verbose=verbose)

        dprint('End: twitter_retrieve_data_streaming_api process', verbose=verbose,
               text_category=Text_Categories.Process_end)

    elif save_data_mode == 'DB':
        if db_hostname is None or db_port is None or db_username is None or db_password is None or db_database is None:
            db_hostname = os.getenv('DB_HOSTNAME')
            db_port = os.getenv('DB_PORT')
            db_username = os.getenv('DB_USER')
            db_password = os.getenv('DB_PASSWORD')
            db_database = os.getenv('DB_DATABASE')
            db_schema = os.getenv('DB_SCHEMA') if os.getenv(
                'DB_SCHEMA') is not None else 'public'

        if db_hostname is None or db_port is None or db_username is None or db_password is None or db_database is None:
            raise Exception(
                "Connection information to PostgreSQL (db_hostname, db_port, db_database, db_user, and db_password) was not provided properly.")

        postgres_tweets = PostgresHandler_Tweets(
            db_hostname, db_port, db_database, db_username, db_password, db_schema)

        dprint('Checking the database ...', verbose=verbose)

        try:
            if postgres_tweets.check_db():
                dprint(F"The database is accessible.", verbose=verbose)
        except:
            dprint('Database is not accessible!', verbose=verbose)
            dprint('-' * 60, verbose=verbose)
            dprint("Unexpected error:", sys.exc_info()[0], verbose=verbose)
            dprint('-' * 60, verbose=verbose)
            traceback.print_decorated_exc(file=sys.stdout)
            dprint('-' * 60, verbose=verbose)
            sys.exit(2)

        _listen_to_tweets(area_name, None, postgres_tweets,
                          save_data_mode, auth, languages, max_num_tweets, only_geotagged, x_min, y_min, x_max, y_max, verbose)


def _listen_to_tweets(area_name, output_folder, postgres, save_data_mode, auth, languages: list, max_num_tweets, only_geotagged, x_min, y_min, x_max, y_max, verbose=False):

    dprint('Initializing the listener ...', verbose=verbose)

    listener = TwitterStreamListener()
    listener.init(area_name=area_name, output_folder=output_folder,
                  postgres=postgres, save_data_mode=save_data_mode, max_num_tweets=max_num_tweets, only_geotagged=only_geotagged, verbose=verbose)
    _stream = Stream(auth, listener)

    stop_flag = False
    while not stop_flag:
        try:
            if len(languages) > 0:
                _stream.filter(languages=languages, locations=[
                    x_min, y_min, x_max, y_max])
            else:
                _stream.filter(locations=[x_min, y_min, x_max, y_max])
            stop_flag = listener.enough_collected()
        except (ProtocolError, AttributeError):
            continue


class TwitterStreamListener(StreamListener):

    def _parse_tweet(self, data, verbose):

        # load JSON item into a dict
        tweet = json.loads(data)

        # check if tweet is valid
        if 'user' in tweet.keys():

            # classify tweet type based on metadata
            if 'retweeted_status' in tweet:
                tweet['TWEET_TYPE'] = 'retweet'

            elif len(tweet['entities']['user_mentions']) > 0:
                tweet['TWEET_TYPE'] = 'mention'

            else:
                tweet['TWEET_TYPE'] = 'tweet'

            return tweet

        else:
            dprint("Error in parsing tweet: %s" %
                   tweet, verbose=verbose, text_category=Text_Categories.Error)

    def enough_collected(self):
        if self.max_num_tweets is None:
            return False
        elif self.num_saved_tweets >= self.max_num_tweets:
            return True
        else:
            return False

    def on_data(self, data):
        insert_list_length = 100
        insert_list_waiting_seconds = 60
        try:
            self.num_received_tweets += 1
            tweet = self._parse_tweet(data, self.verbose)
            now = datetime.datetime.now()

            if self.only_geotagged and tweet['geo'] is None:
                return

            if self.save_data_mode == 'DB':
                self.num_saved_tweets += 1
                self.tweets.append(data)
                print('.', end='')
                time_from_last_insert_seconds = time.time() - self.last_insert_time
                if len(self.tweets) >= insert_list_length or time_from_last_insert_seconds > insert_list_waiting_seconds or self.enough_collected():
                    self.postgres.bulk_insert_tweets(self.tweets, country_code='', bbox_w=0, bbox_e=0, bbox_n=0,
                                                               bbox_s=0, tag='', force_insert=True)
                    self.write_tweet_saved(now)
                    self.tweets.clear()
                    self.last_insert_time = time.time()
                if self.enough_collected():
                    return False

                return True

            else:
                filenameJson = self.output_folder + os.sep + self.area_name.lower() + \
                    os.sep + now.strftime('%Y%m%d-%H') + ".json"
                directory = os.path.dirname(filenameJson)
                if not os.path.exists(directory):
                    os.makedirs(directory)

                with open(filenameJson, 'a') as f:
                    f.write(data)
                    self.num_saved_tweets += 1
                    if self.num_saved_tweets % 10 == 0 or self.enough_collected():
                        self.write_tweet_saved(now)

                    if self.enough_collected():
                        return False

                return True

        except BaseException as e:
            dprint("Error on_data: %s" % str(e), verbose=self.verbose,
                   text_category=Text_Categories.Error)
            return True

    def write_tweet_saved(self, dt):
        dprint(F"{self.save_data_mode}: {str(self.num_saved_tweets)} tweet saved out of {str(self.num_saved_tweets)}, at " +
               dt.strftime("%Y%m%d-%H:%M:%S"), verbose=self.verbose)

    def on_error(self, status):
        if self.enough_collected():
            return False
        else:
            dprint(status)
            return True

    def init(self,  area_name: str, output_folder: str, postgres: PostgresHandler_Tweets, save_data_mode='FILE', only_geotagged=True,
             max_num_tweets: int = None, verbose=False):
        dprint("Intializing Twitter's listener object.")

        self.save_data_mode = save_data_mode
        self.output_folder = output_folder
        self.area_name = area_name
        self.num_received_tweets = 0
        self.num_saved_tweets = 0
        self.postgres = postgres
        self.only_geotagged = only_geotagged
        self.tweets = []
        self.last_insert_time = time.time()
        self.max_num_tweets = max_num_tweets if max_num_tweets is not None and max_num_tweets > 0 else None
        self.verbose = verbose

        if self.save_data_mode == 'DB':
            if self.postgres is None:
                raise Exception('postgres parameter is None.')
        elif self.save_data_mode == 'FILE':
            if self.output_folder is None or self.output_folder == '' or self.area_name is None or self.area_name == '':
                raise Exception(
                    'output_folder parameter, area_name parameter, or both are None/empty.')
        else:
            raise Exception(
                F'The specified save_data_mode ({self.save_data_mode}) is not supported.')
        # self.logger = logging.getLogger(self.output_folder + os.sep + 'log')
        dprint('Initialization was successful.', verbose=verbose)
        dprint('Saving tweets in ' +
               area_name + ' started', verbose=verbose)        
