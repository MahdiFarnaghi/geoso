# -*- coding: UTF8 -*-
"""Console script for geoso.
"""

from logging import warning
import click
from geoso.reader_writer import TweetReaderWriter
from geoso.twitter_data_retrieval import retrieve_data_streaming_api


@click.command()
@click.option('--command', default='help', help='One of the following: jsonl_folder_to_postgres, retrieve_data_streaming_api')
@click.option('--folder', default='', help='The folder path.')
@click.option('--db_username', default='', help='Postgres database username')
@click.option('--db_password', default='', help='Postgres database password')
@click.option('--db_hostname', default='', help='Postgres database hostname')
@click.option('--db_port', default='', help='Postgres database port')
@click.option('--db_database', default='', help='Postgres database name')
@click.option('--db_schema', default='', help='Postgres database schema')

@click.option('--consumer_key', default=None, help='Twitter consumer key')
@click.option('--consumer_secret', default=None, help='Twitter consumer secret')
@click.option('--access_token', default=None, help='Twitter access token')
@click.option('--access_secret', default=None, help='Twitter access secret')
@click.option('--save_data_mode', default='', help='Save data mode. Possible values: FILE (to save the outputs as JSONL files) and DB (to save the data in a PostgreSQL database)')

@click.option('--tweets_output_folder', default=None, help='The output folder to save JSONL files')

@click.option('--area_name', default=None, help='The name of the area for which the tweets are collected. This name will not be used to filter the tweets.')
@click.option('--min_x', default=None, help='Minimum X (longitude) of the area for which the tweets will be retrieved.')
@click.option('--min_y', default=None, help='Minimum Y (latitude) of the area for which the tweets will be retrieved.')
@click.option('--max_x', default=None, help='Maximum X (longitude) of the area for which the tweets will be retrieved.')
@click.option('--max_y', default=None, help='Maximum Y (latitude) of the area for which the tweets will be retrieved.')

@click.option('--languages', default=None, help='Languages of the tweets.')


def main(command, folder, db_username, db_password, db_hostname, db_port, db_database, db_schema, consumer_key, consumer_secret, access_token, access_secret, save_data_mode,
            tweets_output_folder, area_name, min_x, max_x, min_y, max_y, languages):
    """Console script for geoso
    """
    if command == 'help':
        ctx = click.get_current_context()
        click.echo(ctx.get_help())
        ctx.exit()
        
    elif command == 'jsonl_folder_to_postgres':
        TweetReaderWriter.jsonl_folder_to_postgres(folder, db_username=db_username, db_password=db_password, db_hostname=db_hostname,
                                                   db_port=db_port, db_database=db_database, db_schema=db_schema)
    elif command == 'retrieve_data_streaming_api':
        retrieve_data_streaming_api(consumer_key, consumer_secret, access_token, access_secret, save_data_mode,
                                    tweets_output_folder, area_name, min_x, max_x, min_y, max_y, languages, db_hostname, db_port, db_database, db_schema, db_username, db_password)



if __name__ == '__main__':
    main()
