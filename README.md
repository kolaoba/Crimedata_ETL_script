# Simple ETL with Python
This repository contains an ETL pipeline to retrieve crime data from the US gov's `https://crime-data-explorer.fr.cloud.gov/`  using the url - `https://s3-us-gov-west-1.amazonaws.com/cg-d4b776d0-d898-4153-90c8-8336f86bdfec/2022_Quarter_1-Jan-Mar_06-06-2022.zip`.


This data is then loaded in a postgresDB hosted on https://www.elephantsql.com/.

All functions required to run the script available in `main.py` with imports from custom `helper_package` created.

Logs of the process are also kept in `etl.log` file.

## To replicate this project
1. Clone this repo
2. Using your virtual environment manager, install requirements from `requirements.txt`
3. Rename `example_config.py` to `config.py` after updating with your database information.
4. Run `python main.py`.