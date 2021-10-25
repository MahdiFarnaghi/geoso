# -*- coding: UTF8 -*-
"""Console script for geoso.
"""
from logging import warning
import click
import geoso


@click.group()
@click.pass_context
@click.option('--verbose', is_flag=True, help='If provided then the detail description of the process will be printed in the command line.')
def main(ctx, verbose):
    """Console script for geoso 
    """
    # ensure that ctx.obj exists and is a dict (in case `cli()` is called
    # by means other than the `if` block below)
    ctx.ensure_object(dict)
    ctx.obj['verbose'] = verbose

    pass


@main.command()
@click.option("--name", help="Your name :).")
@click.pass_context
def greeting(ctx, name):
    """Test geoso Command Line Interface
    """
    if ctx.obj['verbose']:
        click.echo('Executing ...')
        if name is not None:
            click.echo(f'Hello {name}. Thanks for using geoso.')
        click.echo(f"The geoso library is working properly. :)")
        click.echo(f'version: {geoso.__version__}')
        click.echo('Execution finished successfully.')


@main.command()
@click.pass_context
@click.option('--folder_path', default='', help='The folder path.', type=click.Path(), required=True)
@click.option('--move_imported_to_folder', default=False, help='Move every file that is imported to a sub-folder, named imported.')
@click.option('--continue_on_error', is_flag=True, help='Continue the operation even if an error happens.')
@click.option('--db_username', default='', help='Postgres database username. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--db_password', default='', help='Postgres database password. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--db_hostname', default='', help='Postgres database hostname. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--db_port', default='', help='Postgres database port. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--db_database', default='', help='Postgres database name. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--db_schema', default='public', help='Postgres database schema. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
def twitter_import_jsonl_folder_to_db(ctx, folder_path, move_imported_to_folder, continue_on_error,
                                      db_username, db_password, db_hostname, db_port, db_database, db_schema):
    """Import tweets from a folder of *.jsonl files into the database 
    """
    if ctx.obj['verbose']:
        click.echo('Executing ...')
    geoso.twitter_import_jsonl_folder_to_db(folder_path, move_imported_to_folder=move_imported_to_folder, continue_on_error=continue_on_error, db_username=db_username, db_password=db_password, db_hostname=db_hostname,
                                            db_port=db_port, db_database=db_database, db_schema=db_schema,
                                            verbose=ctx.obj['verbose'])
    if ctx.obj['verbose']:
        click.echo('Execution finished successfully.')


@main.command()
@click.pass_context
@click.option('--file_path', default='', help='The file path.', type=click.Path(), required=True)
@click.option('--continue_on_error', is_flag=True, help='Continue the operation even if an error happens.')
@click.option('--db_username', default='', help='Postgres database username. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--db_password', default='', help='Postgres database password. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--db_hostname', default='', help='Postgres database hostname. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--db_port', default='', help='Postgres database port. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--db_database', default='', help='Postgres database name. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--db_schema', default='public', help='Postgres database schema. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
def twitter_import_jsonl_file_to_db(ctx, file_path, continue_on_error,
                                    db_username, db_password, db_hostname, db_port, db_database, db_schema):
    """Import tweets from a *.jsonl file to the database 
    """
    if ctx.obj['verbose']:
        click.echo('Executing ...')
    geoso.twitter_import_jsonl_file_to_db(file_path, continue_on_error=continue_on_error, db_username=db_username, db_password=db_password, db_hostname=db_hostname,
                                          db_port=db_port, db_database=db_database, db_schema=db_schema,
                                          verbose=ctx.obj['verbose'])
    if ctx.obj['verbose']:
        click.echo('Execution finished successfully.')


@main.command()
@click.pass_context
@click.option('--file_path', default=None, help='The path of input or output file', type=click.Path(), required=True)
@click.option('--start_date', default=None, help='Start date (format: yyyy-mm-dd)', type=click.Path(), required=True)
@click.option('--end_date', default=None, help='End date (format: yyyy-mm-dd)', type=click.Path(), required=True)
@click.option('--x_min', default=None, help='Minimum X (longitude) of the area for which the tweets will be retrieved.', required=True)
@click.option('--y_min', default=None, help='Minimum Y (latitude) of the area for which the tweets will be retrieved.', required=True)
@click.option('--x_max', default=None, help='Maximum X (longitude) of the area for which the tweets will be retrieved.', required=True)
@click.option('--y_max', default=None, help='Maximum Y (latitude) of the area for which the tweets will be retrieved.', required=True)
@click.option('--tag', default=None, help='The tag of the record in the database.')
@click.option('--language', default=None, help='language code of the tweets to be retrieved.')
@click.option('--overwrite_file', is_flag=True, help='Overwrite the file if it exists.')
@click.option('--db_username', default='', help='Postgres database username. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--db_password', default='', help='Postgres database password. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--db_hostname', default='', help='Postgres database hostname. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--db_port', default='', help='Postgres database port. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--db_database', default='', help='Postgres database name. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--db_schema', default='public', help='Postgres database schema. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
def twitter_export_db_to_csv(ctx, file_path, start_date, end_date, x_min, y_min, x_max, y_max, tag, language, overwrite_file,
                             db_username, db_password, db_hostname, db_port, db_database, db_schema):
    """Export tweets from the database to a csv file
    """
    if ctx.obj['verbose']:
        click.echo('Executing ...')
    geoso.twitter_export_db_to_csv(file_path, start_date, end_date, x_min, y_min, x_max, y_max, tag, language, overwrite_file,
                                   db_username, db_password, db_hostname, db_port, db_database, db_schema,
                                   verbose=ctx.obj['verbose'])
    if ctx.obj['verbose']:
        click.echo('Execution finished successfully.')


@main.command()
@click.pass_context
@click.option('--start_date', default=None, help='Start date (format: yyyy-mm-dd)', type=click.Path(), required=False)
@click.option('--end_date', default=None, help='End date (format: yyyy-mm-dd)', type=click.Path(), required=False)
@click.option('--x_min', default=None, help='Minimum X (longitude) of the area for which the tweets will be retrieved.', required=False)
@click.option('--y_min', default=None, help='Minimum Y (latitude) of the area for which the tweets will be retrieved.', required=False)
@click.option('--x_max', default=None, help='Maximum X (longitude) of the area for which the tweets will be retrieved.', required=False)
@click.option('--y_max', default=None, help='Maximum Y (latitude) of the area for which the tweets will be retrieved.', required=False)
@click.option('--tag', default=None, help='The tag of the record in the database.')
@click.option('--language', default=None, help='language code of the tweets to be retrieved.')
@click.option('--db_username', default='', help='Postgres database username. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--db_password', default='', help='Postgres database password. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--db_hostname', default='', help='Postgres database hostname. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--db_port', default='', help='Postgres database port. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--db_database', default='', help='Postgres database name. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--db_schema', default='public', help='Postgres database schema. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
def twitter_get_tweets_info_from_db(ctx, start_date, end_date, x_min, y_min, x_max, y_max, tag, language,
                                    db_username, db_password, db_hostname, db_port, db_database, db_schema):
    """Get information about tweets in the database
    """
    if ctx.obj['verbose']:
        click.echo('Executing ...')

    result = geoso.twitter_get_tweets_info_from_db(start_date=start_date, end_date=end_date, x_min=x_min, y_min=y_min, x_max=x_max, y_max=y_max, tag=tag, language=language,
                                                   db_username=db_username, db_password=db_password, db_hostname=db_hostname, db_port=db_port, db_database=db_database,
                                                   db_schema=db_schema,
                                                   verbose=ctx.obj['verbose'])

    print(result.transpose().to_string())
    if ctx.obj['verbose']:
        click.echo('Execution finished successfully.')


@main.command()
@click.pass_context
@click.option('--consumer_key', default=None, help='Twitter consumer key. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--consumer_secret', default=None, help='Twitter consumer secret. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--access_token', default=None, help='Twitter access token. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--access_secret', default=None, help='Twitter access secret. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--save_data_mode', default='FILE', type=click.Choice(['FILE', 'DB'], case_sensitive=False), help='Save data mode. Possible values: FILE (to save the outputs as JSONL files) and DB (to save the data in a PostgreSQL database). The default value is FILE.')
@click.option('--tweets_output_folder', default=None, help='The output folder to save JSONL files. Required if save_date_mode is set to FILE. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.', type=click.Path())
@click.option('--area_name', default=None, help='The name of the area for which the tweets are collected. This name will not be used to filter the tweets. ')
@click.option('--x_min', default=None, help='Minimum X (longitude) of the area for which the tweets will be retrieved. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--y_min', default=None, help='Minimum Y (latitude) of the area for which the tweets will be retrieved. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--x_max', default=None, help='Maximum X (longitude) of the area for which the tweets will be retrieved. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--y_max', default=None, help='Maximum Y (latitude) of the area for which the tweets will be retrieved. This variable is required. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--languages', default=None, help='language codes of the tweets to be retrieved. If not provided, no filter will be applied on the languages of the tweets.')
@click.option('--max_num_tweets', default=None, help='Maximum number of tweets. The data collection will be finished after collecting the specified number. If not provided, the data collection never stops')
@click.option('--only_geotagged', is_flag=True, help='If provided, geoso will only collect geotagged tweets')
@click.option('--db_username', default='', help='Postgres database username. Required if save_date_mode is set to DB. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--db_password', default='', help='Postgres database password. Required if save_date_mode is set to DB. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--db_hostname', default='', help='Postgres database hostname. Required if save_date_mode is set to DB. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--db_port', default='', help='Postgres database port. Required if save_date_mode is set to DB. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--db_database', default='', help='Postgres database name. Required if save_date_mode is set to DB. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
@click.option('--db_schema', default='public', help='Postgres database schema. Required if save_date_mode is set to DB. If it is not provided geoso tries to load it from an environmental variable with the same name, but capitalized.')
def twitter_retrieve_data_streaming_api(ctx, consumer_key, consumer_secret, access_token, access_secret, save_data_mode, tweets_output_folder, area_name,
                                        x_min, y_min, x_max, y_max, languages, max_num_tweets, only_geotagged,
                                        db_username, db_password, db_hostname, db_port, db_database, db_schema):
    """Retrieve data from Twitter Streaming API
    """

    if ctx.obj['verbose']:
        click.echo('Executing ...')
    geoso.twitter_retrieve_data_streaming_api(consumer_key, consumer_secret, access_token, access_secret, str(save_data_mode).upper(),
                                              tweets_output_folder, area_name, float(x_min), float(x_max), float(
                                                  y_min), float(y_max), languages, int(max_num_tweets),
                                              only_geotagged, db_hostname, db_port, db_database, db_schema, db_username, db_password)
    if ctx.obj['verbose']:
        click.echo('Execution finished successfully.')


if __name__ == '__main__':
    main()
