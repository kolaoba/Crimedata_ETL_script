import pandas as pd
import requests
from helper_package.helper_functions import *
import glob
from sqlalchemy import create_engine
from config import DB_URL
import logging
from datetime import datetime

#initialise logging file
logging.basicConfig(filename='etl.log', encoding='utf-8', level=logging.DEBUG)

# data source
url='https://s3-us-gov-west-1.amazonaws.com/cg-d4b776d0-d898-4153-90c8-8336f86bdfec/2022_Quarter_1-Jan-Mar_06-06-2022.zip'

def fetch_data_from_url(url):
    """
    Function creates download folder, retrieves data from given url, extracts filename, saves file,
    unzips file, deletes zipped file, loads data into memory
    """

    # create downloads folder
    folder_path = create_downloads_folder()
    # get from from url
    req = requests.get(url)
    # extract filename
    zip_name, zip_path = extract_filename(url, folder_path)
    # save file
    save_file(req, zip_path)
    logging.info('%s - %s saved successfully to %s' % (datetime.now(), zip_name, zip_path))
    # unzip dodwnloaded file
    unzip_file(zip_path, folder_path)
    # Remove zip files
    os.remove(zip_path)
    # remove file extention to get file name
    file_name = zip_name[:-4]
    # find xlsx file in downloads folder
    file_path = glob.glob('./downloads/*.xlsx')
    # load xlsx file
    df = pd.read_excel(file_path[0])

    return df, file_name



def load_data_into_db(df, file_name):
    # intiialise db connection
    engine = create_engine(DB_URL)

    with engine.connect() as conn:
        # load data into database with table_name as filename
        df.to_sql(file_name, con=conn, index=False, if_exists='replace', chunksize=100)
        logging.info('%s - %s successfully loaded into DB!' % (datetime.now(), file_name))

        test_query = '''SELECT * FROM \"%s\" LIMIT 5;''' % file_name
        data = pd.read_sql(test_query, conn)
        print(data)




def main():
    logging.info('%s - Starting ETL Process' % datetime.now())
    # fetch data from source
    df, file_name = fetch_data_from_url(url=url)
    # load data into database
    load_data_into_db(df, file_name)
    logging.info('%s - ETL process completed successfully!' % datetime.now())

























if __name__ == '__main__':
    main()