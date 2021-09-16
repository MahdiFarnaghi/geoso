from .postgres import PostgresHandler_Tweets
from .utils import Folders
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


def retrieve_data_streaming_api(consumer_key=None, consumer_secret=None, access_token=None, access_secret=None, save_data_mode=None,
                                tweets_output_folder=None, area_name='',  min_x=None, max_x=None, min_y=None, max_y=None,
                                languages=None, db_hostname=None, db_port=None, db_database=None, db_schema='Public', db_user=None, db_password=None):
    load_dotenv()

    print(60*"*")
    print('Start Tweet Listener.')
    print(60*"=")

    if save_data_mode is None:
        save_data_mode = os.getenv('SAVE_DATA_MODE')
        if save_data_mode is None:
            save_data_mode = 'FILE'
    print(f"Save data mode: {save_data_mode}")

    if consumer_key is None or consumer_secret is None or access_token is None or access_secret is None:
        consumer_key = os.getenv('CONSUMER_KEY')
        consumer_secret = os.getenv('CONSUMER_SECRET')
        access_token = os.getenv('ACCESS_TOKEN')
        access_secret = os.getenv('ACCESS_SECRET')

    if consumer_key is None or consumer_secret is None or access_token is None or access_secret is None:
        raise Exception(
            "Twitter authentication information (consumer_key, consumer_secret, access_token, and access_secret) was not provided properly.")

    print('Authentication ...')
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    api = tweepy.API(auth)
    print('Authentication was successful.')

    if min_x is None or min_y is None or max_x is None or max_y is None:
        min_x = float(os.getenv('MIN_X')) if os.getenv(
            'MIN_X') is not None else None
        max_x = float(os.getenv('MAX_X')) if os.getenv(
            'MAX_X') is not None else None
        min_y = float(os.getenv('MIN_Y')) if os.getenv(
            'MIN_Y') is not None else None
        max_y = float(os.getenv('MAX_Y')) if os.getenv(
            'MAX_Y') is not None else None

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

    if save_data_mode == 'FILE':
        if languages != '':
            languages = str(languages).split(',')
            languages = [str.strip(lang) for lang in languages]

        _listen_to_tweets(area_name, tweets_output_folder, None,
                          save_data_mode, auth, languages, min_x, min_y, max_x, max_y)

    elif save_data_mode == 'DB':
        if db_hostname is None or db_port is None or db_user is None or db_password is None or db_database is None:
            db_hostname = os.getenv('DB_HOSTNAME')
            db_port = os.getenv('DB_PORT')
            db_user = os.getenv('DB_USER')
            db_password = os.getenv('DB_PASSWORD')
            db_database = os.getenv('DB_DATABASE')
            db_schema = os.getenv('DB_SCHEMA') if os.getenv(
                'DB_SCHEMA') is not None else 'public'

        if db_hostname is None or db_port is None or db_user is None or db_password is None or db_database is None:
            raise Exception(
                "Connection information to PostgreSQL (db_hostname, db_port, db_database, db_user, and db_password) was not provided properly.")

        postgres_tweets = PostgresHandler_Tweets(
            db_hostname, db_port, db_database, db_user, db_password, db_schema)

        print('Checking the database ...')

        try:
            if postgres_tweets.check_db():
                print(F"The database is accessible.")
        except:
            print('Database is not accessible!')
            print('-' * 60)
            print("Unexpected error:", sys.exc_info()[0])
            print('-' * 60)
            traceback.print_exc(file=sys.stdout)
            print('-' * 60)
            sys.exit(2)

        _listen_to_tweets(area_name, None, postgres_tweets,
                          save_data_mode, auth, languages, min_x, min_y, max_x, max_y)


def _listen_to_tweets(area_name, output_folder, postgres, save_data_mode, auth, languages: list, min_x, min_y, max_x, max_y):
    print(60*"*")

    print('Initializing the listener ...')
    print(f"BBOX ({area_name}): {min_x}, {min_y}, {max_x}, {max_y}")
    print(f"Languages: {', '.join(languages)}")
    listener = TwitterStreamListener()
    listener.init(area_name=area_name,  output_folder=output_folder,
                  postgres=postgres, save_data_mode=save_data_mode)
    WorldStream = Stream(auth, listener)
    # if len(languages) > 0:
    #     WorldStream.filter(languages=languages, locations=[min_x, min_y, max_x, max_y])
    # else:
    #     WorldStream.filter(locations=[min_x, min_y, max_x, max_y])
    while True:
        try:
            if len(languages) > 0:
                WorldStream.filter(languages=languages, locations=[
                                   min_x, min_y, max_x, max_y])
            else:
                WorldStream.filter(locations=[min_x, min_y, max_x, max_y])
        except (ProtocolError, AttributeError):
            continue


def _parse_tweet(data):

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
        print("Error in parsing tweet: %s" % tweet)
        # logger.warning("Incomplete tweet: %s", tweet)


class TwitterStreamListener(StreamListener):

    def on_data(self, data):
        insert_list_length = 100
        insert_list_waiting_seconds = 60
        try:
            self.tweetnumber += 1
            tweet = _parse_tweet(data)
            now = datetime.datetime.now()

            if self.save_geotweets and tweet['geo'] is None:
                return

            if self.save_data_mode == 'DB':
                self.geotweetnumber += 1
                self.tweets.append(data)
                print('.', end='')
                time_from_last_insert_seconds = time.time() - self.last_insert_time
                if len(self.tweets) >= insert_list_length or time_from_last_insert_seconds > insert_list_waiting_seconds:
                    self.postgres.bulk_insert_geotagged_tweets(self.tweets, country_code='', bbox_w=0, bbox_e=0, bbox_n=0,
                                                               bbox_s=0, tag='', force_insert=True)
                    print(
                        f"\n{len(self.tweets)} tweets were inserted into the database.")
                    self.tweets.clear()
                    self.last_insert_time = time.time()

                # if self.postgres.upsert_tweet(data):
                #     self.write_tweet_saved(
                #         self.geotweetnumber, self.tweetnumber, self.save_data_mode, now)
                # else:
                #     print('The tweet could not be saved.')
            else:
                #print('Tweet number ' + str(self.tweetnumber) + ' in ' + self.area_name)
                filenameJson = self.output_folder + os.sep + self.area_name.lower() + \
                    os.sep + now.strftime('%Y%m%d-%H') + ".json"
                directory = os.path.dirname(filenameJson)
                if not os.path.exists(directory):
                    os.makedirs(directory)

                # print(tweet['text'].encode('unicode_escape'))
                with open(filenameJson, 'a') as f:
                    f.write(data)
                    self.geotweetnumber += 1
                    self.write_tweet_saved(
                        self.geotweetnumber, self.tweetnumber, self.save_data_mode, now)
                    return True

        except BaseException as e:
            print("Error on_data: %s" % str(e))
            # traceback.print_exc(file=sys.stdout)
            #print('No GeoTag, not saved.')
            # print("----------------------")
        return True

    def write_tweet_saved(self, geotweetnumber, tweetnumber, save_data_mode, dt):
        # print("----------------------")
        print(F"{save_data_mode}: {str(geotweetnumber)} tweet saved out of {str(tweetnumber)}, at " +
              dt.strftime("%Y%m%d-%H:%M:%S"))
        # print(self.postgres.number_of_tweets())
        # print("----------------------")

    def on_error(self, status):
        print(status)
        return True

    def init(self,  area_name: str, output_folder: str, postgres: PostgresHandler_Tweets, save_data_mode='FILE', save_geotweets=True):
        print("Intializing Twitter's listener object.")
       
        self.save_data_mode = save_data_mode
        self.output_folder = output_folder
        self.area_name = area_name
        self.tweetnumber = 0
        self.geotweetnumber = 0
        self.postgres = postgres
        self.save_geotweets = save_geotweets
        self.tweets = []
        self.last_insert_time = time.time()

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
        print('Initialization was successful.\n\n\n')
        print('+'*60)
        print('Saving geotagged tweets in ' + area_name + ' started')
        print('+'*60)
        print('\n')
