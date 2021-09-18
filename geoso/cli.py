# -*- coding: UTF8 -*-
"""Console script for geoso.
"""

from logging import warning
import click
from geoso import twitter_import_jsonl_folder_to_postgres, twitter_import_jsonl_folder_to_postgres, twitter_export_postgres_to_csv


@click.group()
@click.pass_context
@click.option('--verbose', is_flag=True, help='If provided then the detail description of the process will be printed in the command line.')
def main(ctx, verbose):
    '''Console script for geoso 
    '''
    # ensure that ctx.obj exists and is a dict (in case `cli()` is called
    # by means other than the `if` block below)
    ctx.ensure_object(dict)
    ctx.obj['verbose'] = verbose

    pass


@main.command()
@click.pass_context
@click.option('--folder_path', default='', help='The folder path.', type=click.Path(), required=True)
@click.option('--move_imported_to_folder', default=False, help='Move every file that is imported to a sub-folder, named imported.')
@click.option('--continue_on_error', default=True, help='Continue the operation even if an error happens.')
@click.option('--db_username', default='', help='Postgres database username. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--db_password', default='', help='Postgres database password. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--db_hostname', default='', help='Postgres database hostname. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--db_port', default='', help='Postgres database port. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--db_database', default='', help='Postgres database name. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--db_schema', default='public', help='Postgres database schema. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
def import_jsonl_folder_to_postgres(ctx, folder_path, move_imported_to_folder, continue_on_error,
                                    db_username, db_password, db_hostname, db_port, db_database, db_schema):
    if ctx.obj['verbose']:
        click.echo('Executing import_jsonl_folder_to_postgres')
    twitter_import_jsonl_folder_to_postgres(folder_path, move_imported_to_folder=move_imported_to_folder, continue_on_error=continue_on_error, db_username=db_username, db_password=db_password, db_hostname=db_hostname,
                                                      db_port=db_port, db_database=db_database, db_schema=db_schema,
                                                      verbose=ctx.obj['verbose'])


@main.command()
@click.pass_context
@click.option('--file_path', default=None, help='The path of input or output file', type=click.Path(), required=True)
@click.option('--start_date', default=None, help='Start date (format: yyyy-mm-dd)', type=click.Path(), required=True)
@click.option('--end_date', default=None, help='End date (format: yyyy-mm-dd)', type=click.Path(), required=True)
@click.option('--min_x', default=None, help='Minimum X (longitude) of the area for which the tweets will be retrieved.', required=True)
@click.option('--min_y', default=None, help='Minimum Y (latitude) of the area for which the tweets will be retrieved.', required=True)
@click.option('--max_x', default=None, help='Maximum X (longitude) of the area for which the tweets will be retrieved.', required=True)
@click.option('--max_y', default=None, help='Maximum Y (latitude) of the area for which the tweets will be retrieved.', required=True)
@click.option('--table_name', default='tweet', help='The name of the source table in the database.')
@click.option('--tag', default=None, help='The tag of the record in the database.')
@click.option('--language', default=None, help='language code of the tweets to be retrieved.')
@click.option('--overwrite_file', default=False, help='Overwrite the file if it exists.')
@click.option('--db_username', default='', help='Postgres database username. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--db_password', default='', help='Postgres database password. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--db_hostname', default='', help='Postgres database hostname. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--db_port', default='', help='Postgres database port. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--db_database', default='', help='Postgres database name. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--db_schema', default='public', help='Postgres database schema. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
def export_postgres_to_csv(ctx, file_path, start_date, end_date, min_x, min_y, max_x, max_y, table_name, tag, language, overwrite_file,
                           db_username, db_password, db_hostname, db_port, db_database, db_schema):
    if ctx.obj['verbose']:
        click.echo('Executing export_postgres_to_csv')
    twitter_export_postgres_to_csv(file_path, start_date, end_date, min_x, min_y, max_x, max_y, table_name, tag, language, overwrite_file,
                                             db_username, db_password, db_hostname, db_port, db_database, db_schema,
                                             ctx.obj['verbose'])


@main.command()
@click.pass_context
@click.option('--consumer_key', default=None, help='Twitter consumer key. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--consumer_secret', default=None, help='Twitter consumer secret. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--access_token', default=None, help='Twitter access token. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--access_secret', default=None, help='Twitter access secret. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--save_data_mode', default='FILE', type=click.Choice(['FILE', 'DB'], case_sensitive=False), help='Save data mode. Possible values: FILE (to save the outputs as JSONL files) and DB (to save the data in a PostgreSQL database). The default value is FILE.')
@click.option('--tweets_output_folder', default=None, help='The output folder to save JSONL files. Required if save_date_mode is set to FILE. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.', type=click.Path())
@click.option('--area_name', default=None, help='The name of the area for which the tweets are collected. This name will not be used to filter the tweets. ')
@click.option('--min_x', default=None, help='Minimum X (longitude) of the area for which the tweets will be retrieved. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--min_y', default=None, help='Minimum Y (latitude) of the area for which the tweets will be retrieved. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--max_x', default=None, help='Maximum X (longitude) of the area for which the tweets will be retrieved. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--max_y', default=None, help='Maximum Y (latitude) of the area for which the tweets will be retrieved. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--language', default=None, help='language code of the tweets to be retrieved.')
@click.option('--db_username', default='', help='Postgres database username. Required if save_date_mode is set to DB. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--db_password', default='', help='Postgres database password. Required if save_date_mode is set to DB. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--db_hostname', default='', help='Postgres database hostname. Required if save_date_mode is set to DB. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--db_port', default='', help='Postgres database port. Required if save_date_mode is set to DB. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--db_database', default='', help='Postgres database name. Required if save_date_mode is set to DB. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--db_schema', default='public', help='Postgres database schema. Required if save_date_mode is set to DB. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
def retrieve_data_streaming_api(ctx, consumer_key, consumer_secret, access_token, access_secret, save_data_mode, tweets_output_folder, area_name, min_x, min_y, max_x, max_y, language,
                                db_username, db_password, db_hostname, db_port, db_database, db_schema):
    if ctx.obj['verbose']:
        click.echo('Executing retrieve_data_streaming_api')
    retrieve_data_streaming_api(consumer_key, consumer_secret, access_token, access_secret, str(save_data_mode).upper(),
                                tweets_output_folder, area_name, min_x, max_x, min_y, max_y, language, db_hostname, db_port, db_database, db_schema, db_username, db_password)


if __name__ == '__main__':
    main()
