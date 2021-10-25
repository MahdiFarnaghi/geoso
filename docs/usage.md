# Usage

`geoso` can be used in two different ways.

-   [Through the command line interface (CLI)](#using-geoso-in-a-python-script)
-   [In a python script](#using-geoso-cli)

## Using `geoso` in a python script

To use geoso in a project:

```
import geoso
```

## Using `geoso` CLI

After installing `geoso`, e.g., in a python environment, you can simply execute geoso in command line to get access to provided functionality.

```console 
(base) mf@ut: geoso
```

To see the possible commands under geoso CLI, use the following command:

```console 
(base) mf@ut: geoso --help
```

### Retrieve tweets from the Streaming API

To retrieve tweets for a particular bounding box from Twitter Streaming API and save it in a PostgreSQL database (with PostGIS extension) or in a folder as [JSON Lines files](https://jsonlines.org/), use the following command: 

```console 
geoso --verbose twitter-retrieve-data-streaming-api
``` 

You should provide the proper input options for the command to work properly. To see the list of available options, execute the command with `--help` option, as follows:

```console 
geoso --verbose twitter-retrieve-data-streaming-api --help
``` 

### Retrieve information about the tweets that were saved in the database

To retrieve information about the tweets that have been already saved in the database, use the following command. 

```console 
geoso --verbose twitter-get-tweets-info-from-db
``` 

You should provide the proper input options for the command to work properly. To see the list of available options, execute the command with `--help` option.

### Export tweets from the database to a CSV file

To export tweets from the database to a [CSV (comma separated values) file](https://en.wikipedia.org/wiki/Comma-separated_values), use the following command:

```console 
geoso --verbose twitter-export-db-to-csv
``` 

You should provide the proper input options for the command to work properly. To see the list of available options, execute the command with `--help` option.

### Import tweets from [JSON Lines files](https://jsonlines.org/) to the database

Whenever you have some tweets saved in a [JSON Lines file](https://jsonlines.org/), you can use the following command to import those tweets into your PostgreSQL database:

```console 
geoso --verbose twitter-import-jsonl-file-to-db
``` 

If you have more than one [JSON Lines file](https://jsonlines.org/), saved in a folder on your machine, the following command can be used to import them collectively into your database:

```console 
geoso --verbose twitter-import-jsonl-folder-to-db
``` 

You should provide the proper input options for above commands to work properly. To see the list of available options, execute the command with `--help` option.
