from geoso.clean_text import twitter_clean_text_in_dataframe
from geoso.postgres import PostgresHandler_Tweets
import unittest
import os
from pathlib import Path
import csv
import traceback
import sqlalchemy_utils
from sqlalchemy_utils.functions.database import drop_database
from geoso import utils
import shutil
import pandas as pd

from geoso.utils import EnvVar, Folders
from geoso import twitter_clean_text, twitter_import_jsonl_folder_to_db, twitter_import_jsonl_folder_to_db, twitter_export_db_to_csv, twitter_retrieve_data_streaming_api, twitter_import_jsonl_file_to_db, twitter_get_tweets_info_from_db
from test_helper import drop_create_database, drop_database, test_data_jsonl_folder_path, test_data_jsonl_file_path


class Test_Twitter(unittest.TestCase):
    """Tests for `geoso.twitter` module."""

    def setUp(self) -> None:
        self.DB_HOSTNAME, self.DB_PORT, self.DB_USERNAME, self.DB_PASSWORD, self.DB_DATABASE, self.DB_SCHEMA = EnvVar.get_test_db_env_variables()
        drop_create_database(self.DB_HOSTNAME, self.DB_PORT, self.DB_USERNAME,
                             self.DB_PASSWORD, self.DB_DATABASE, self.DB_SCHEMA)

    def tearDown(self):
        drop_database(self.DB_HOSTNAME, self.DB_PORT, self.DB_USERNAME,
                      self.DB_PASSWORD, self.DB_DATABASE, self.DB_SCHEMA)


    def test_clean_text(self):

        texts = [
            ("@wahlborn @YouTube @TobiasDirking @SimoNiedermeyer Aber ohne Auto  brauche sowas nicht  bin ja ein DENKENDER Mensch ðŸ˜Ž", 
            'de', 
            'aber ohne auto brauche sowas nicht bin ja ein denkender mensch ðÿ˜ž'),
            ("I'm at @HolidayInn Brighton - Seafront in BRIGHTON, Brighton and Hove https:\/\/t.co\/IkuY3l7HEf",
             'en', 
             "i m at brighton seafront in brighton and hove"),
            ("A esta hora iniciamos con la toma empresarial en el municipio del El Espinal, donde invitamos a todos a participar\u2026 https:\/\/t.co\/5zXDuRryjy",
            'es', 
            "a esta hora iniciamos con la toma empresarial en el municipio del espinal donde invitamos a todos a participar"),
            ("Interested in a job in #SandySprings, GA? This could be a great fit. Click the link in our bio to apply: CNA-PRN Re\u2026 https:\/\/t.co\/QIkGEJpkvu",
            'en', 
            "interested in a job in sandy springs ga this could be a great fit click the link in our bio to apply cna prn re"),
            ("On a little gravel trail along the Hudson River.\n\n#bikepacking #ridemoreexplore #OutsideIsFree #everyrideisepic\u2026 https:\/\/t.co\/AcEd1NAYkg",
            'en', 
            'on a little gravel trail along the hudson river bikepacking ridemoreexplore outside is free everyrideisepic'),
            ("Ais pun dah cair duduk dengan awak apatah lagi saya \ud83d\ude18\n\n#samsamlukupalamau @ Tasek Gelugor, Pulau Pinang, Malaysia https:\/\/t.co\/ZYPCmEQbWb", 
            'in', 
            '')
        ]

        for r in texts:
            text_cleaned = twitter_clean_text(r[0], r[1])            
            assert text_cleaned == r[2]

        df = pd.DataFrame(texts, columns=['text', 'lang', 'expected_text'])

        df['text_clean'] = twitter_clean_text_in_dataframe(df, text_column='text', lang_code_column='lang')
        assert df.text_clean[0] == texts[0][2]
        assert df.text_clean[4] == texts[4][2]


    def test_jsonl_folder_to_db(self):

        num_inserted = twitter_import_jsonl_folder_to_db(
            test_data_jsonl_folder_path,
            continue_on_error=True,
            db_database=self.DB_DATABASE,
            db_hostname=self.DB_HOSTNAME,
            db_password=self.DB_PASSWORD,
            db_port=self.DB_PORT,
            db_schema=self.DB_SCHEMA,
            db_username=self.DB_USERNAME)

        assert num_inserted > 0

    def test_get_tweets_info_from_db(self):
        num_inserted = twitter_import_jsonl_file_to_db(
            test_data_jsonl_file_path,
            continue_on_error=True,
            db_database=self.DB_DATABASE,
            db_hostname=self.DB_HOSTNAME,
            db_password=self.DB_PASSWORD,
            db_port=self.DB_PORT,
            db_schema=self.DB_SCHEMA,
            db_username=self.DB_USERNAME)

        assert num_inserted > 0

        start_date = '2019-01-01'
        end_date = '2019-12-30'
        x_min = -180
        x_max = 180
        y_min = -90
        y_max = 90

        df = twitter_get_tweets_info_from_db(
            start_date=start_date,
            end_date=end_date,
            x_min=x_min,
            x_max=x_max,
            y_min=x_min,
            y_max=y_max,
            db_database=self.DB_DATABASE,
            db_hostname=self.DB_HOSTNAME,
            db_password=self.DB_PASSWORD,
            db_port=self.DB_PORT,
            db_schema=self.DB_SCHEMA,
            db_username=self.DB_USERNAME)

        assert df.iloc[0, 0] > 0

        df = twitter_get_tweets_info_from_db(
            start_date=start_date,
            end_date=end_date,
            db_database=self.DB_DATABASE,
            db_hostname=self.DB_HOSTNAME,
            db_password=self.DB_PASSWORD,
            db_port=self.DB_PORT,
            db_schema=self.DB_SCHEMA,
            db_username=self.DB_USERNAME)

        assert df.iloc[0, 0] > 0

        df = twitter_get_tweets_info_from_db(
            x_min=x_min,
            x_max=x_max,
            y_min=x_min,
            y_max=y_max,
            db_database=self.DB_DATABASE,
            db_hostname=self.DB_HOSTNAME,
            db_password=self.DB_PASSWORD,
            db_port=self.DB_PORT,
            db_schema=self.DB_SCHEMA,
            db_username=self.DB_USERNAME)

        assert df.iloc[0, 0] > 0

        df = twitter_get_tweets_info_from_db(
            db_database=self.DB_DATABASE,
            db_hostname=self.DB_HOSTNAME,
            db_password=self.DB_PASSWORD,
            db_port=self.DB_PORT,
            db_schema=self.DB_SCHEMA,
            db_username=self.DB_USERNAME)

        assert df.iloc[0, 0] > 0

    def test_export_db_to_csv(self):

        num_inserted = twitter_import_jsonl_folder_to_db(
            test_data_jsonl_folder_path,
            continue_on_error=True,
            db_database=self.DB_DATABASE,
            db_hostname=self.DB_HOSTNAME,
            db_password=self.DB_PASSWORD,
            db_port=self.DB_PORT,
            db_schema=self.DB_SCHEMA,
            db_username=self.DB_USERNAME)

        if num_inserted <= 0:
            assert False
        else:
            file_path = os.path.join(Folders.get_temp_folder(), 'test.csv')
            twitter_export_db_to_csv(file_path=file_path,
                                     start_date='2019-01-01',
                                     end_date='2019-12-30',
                                     x_min=-180,
                                     y_min=-90,
                                     x_max=180,
                                     y_max=90,
                                     db_database=self.DB_DATABASE,
                                     db_hostname=self.DB_HOSTNAME,
                                     db_password=self.DB_PASSWORD,
                                     db_port=self.DB_PORT,
                                     db_schema=self.DB_SCHEMA,
                                     db_username=self.DB_USERNAME

                                     )
            cnt_rows = 0
            with open(file_path) as csvFile:
                reader = csv.DictReader(csvFile)
                for row in reader:
                    cnt_rows = cnt_rows + 1
            success = cnt_rows == num_inserted

            try:
                os.remove(file_path)
                os.rmdir(Path(file_path).parent)
            except:
                pass

            assert success
