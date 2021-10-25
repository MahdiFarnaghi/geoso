# geoso

[![image](https://img.shields.io/pypi/v/geoso.svg)](https://pypi.python.org/pypi/geoso)
[![image](https://github.com/mahdifarnaghi/geoso/workflows/docs/badge.svg)](https://geemap.org)
[![image](https://github.com/mahdifarnaghi/geoso/workflows/build/badge.svg)](https://github.com/mahdifarnaghi/geoso/actions?query=workflow%3Abuild)
[![image](https://img.shields.io/twitter/follow/mahdifarnaghi?style=social)](https://twitter.com/mahdifarnaghi)
[![image](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**A Python package for collecting and spatio-temporal analysis of social media contents**

-   GitHub repo: <https://github.com/MahdiFarnaghi/geoso>
-   PyPI: https://pypi.org/project/geoso/
-   Documentation: <https://MahdiFarnaghi.github.io/geoso>
-   Free software: MIT license

## Introduction

**geoso** is a Python library, being developed to facilitate collection, cleansing, and spatial and spatio-temporal analysis of social media data.

The vision is that the library provided the possibility to download geo-tagged social media content into a database, e.g., PostgreSQL, preprocess the stored data, retrieve, and analyse the data.

## Features

-   Twitter
    -   Download tweets from Twitter Streaming API and save them into either a database or [JSON Lines](https://jsonlines.org/) text files.
    -   Import tweets that were from [JSON Lines](https://jsonlines.org/) text files into the database.
    -   Export tweets to CSV file.
    -   Clean tweets text in the database.
    <!-- - Detect tweets that were published by bots. -->
    -   Retrieve tweets from the database as [pandas DataFrame](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html).

## Under development features

-   Twitter
    -   Clean tweets text in the database.
    -   Detect tweets that were published by bots.

## Scientific publications

If you are using this library, the following scientific publications could be of your interest.

-   Farnaghi, M., Ghaemi, Z., & Mansourian, A. (2020). [Dynamic Spatio-Temporal Tweet Mining for Event Detection: A Case Study of Hurricane Florence](https://doi.org/10.1007/s13753-020-00280-z). International Journal of Disaster Risk Science, 11, 378-393.

-   Ostermann, F. O. (2021). [Linking geosocial sensing with the socio-demographic fabric of smart cities. ISPRS international journal of geo-information](https://doi.org/10.3390/ijgi10020052), 10(2), 1-22. [52].

-   Zahra, K., Imran, M., Ostermann, F. O. (2020). [Automatic identification of eyewitness messages on twitter during disasters](https://doi.org/10.1016/j.ipm.2019.102107). Information processing & management 57 (1), 102107

-   Ghaemi, Z. & Farnaghi, M. 2019. [A Varied Density-based Clustering Approach for Event Detection from Heterogeneous Twitter Data.](https://doi.org/10.1007/s13753-020-00280-z) ISPRS International Journal of Geo-Information, 8 (2).

## Credits

-   [spaCy](https://spacy.io/) is used for cleaning texts.
-   [Tweepy](https://www.tweepy.org/) is used to develop Twitter data retrieval functionalities.
-   This package was created with [Cookiecutter](https://github.com/cookiecutter/cookiecutter) and the [giswqs/pypackage](https://github.com/giswqs/pypackage) project template and instructions from [Python Packages](https://py-pkgs.org/) book.
