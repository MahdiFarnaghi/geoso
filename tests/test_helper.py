import unittest
import os
from pathlib import Path
import csv
import traceback
import sqlalchemy_utils
from sqlalchemy_utils.functions.database import drop_database

from geoso.utils import EnvVar, Folders, print_error
from geoso.reader_writer import TweetReaderWriter
from geoso.postgres import PostgresHandler_Tweets
from geoso.postgres import PostgresHandler

def drop_database(DB_HOSTNAME, DB_PORT, DB_USERNAME, DB_PASSWORD, DB_DATABASE, DB_SCHEMA):
    try:
        postgres = PostgresHandler(
            DB_HOSTNAME, DB_PORT, DB_DATABASE, DB_USERNAME, DB_PASSWORD, DB_SCHEMA)
        sqlalchemy_utils.functions.drop_database(postgres.db_url)
    except:        
        pass
    

def drop_create_database(DB_HOSTNAME, DB_PORT, DB_USERNAME, DB_PASSWORD, DB_DATABASE, DB_SCHEMA):
    postgres = PostgresHandler(
        DB_HOSTNAME, DB_PORT, DB_DATABASE, DB_USERNAME, DB_PASSWORD, DB_SCHEMA)

    try:
        sqlalchemy_utils.functions.drop_database(postgres.db_url)
    except:
        pass
    pass

    postgres = PostgresHandler(
        DB_HOSTNAME, DB_PORT, DB_DATABASE, DB_USERNAME, DB_PASSWORD, DB_SCHEMA)    
