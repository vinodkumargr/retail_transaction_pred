import pandas as pd
import os, sys
from Retail_transcation.exception import RetailException
from Retail_transcation.logger import logging
from Retail_transcation import config
import yaml


def get_as_df(self, database_name:str , collection_name:str)-> pd.DataFrame:
    try:
        logging.info("Reading data from mongoDB")
        df = pd.DataFrame(list(config.mongo_client[database_name][collection_name].find()))
        
        logging.info("Found data shape : {df.shape}")
        if "_id" in df.columns:
            df.drop(['_id'], axis=1, inplsce=True)
            logging.info("Found _id and dropped")
        logging.info("data shape : {df.shape}")

        return df
    

def write_into_yaml(self, file_path , data : dict):

    try:
        file_dir=os.path.dirname(file_path)
        os.makedirs(file_dir, exist_ok=True)
        with open(file_dir, "w") as write:
            yaml.dump(data, write)
            logging("written into yaml")

    except Exception as e:
        raise RetailException(e, sys)

