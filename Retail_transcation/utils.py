import pandas as pd
import numpy as np
import os, sys
from Retail_transcation.exception import RetailException
from Retail_transcation.logger import logging
from Retail_transcation import config
import yaml, dill


def get_as_df(database_name:str , collection_name:str)-> pd.DataFrame:
    try:
        logging.info("Reading data from mongoDB")
        df = pd.DataFrame(list(config.mongo_client[database_name][collection_name].find()))
        
        logging.info(f"Found data shape : {df.shape}")
        if "_id" in df.columns:
            df.drop(['_id'], axis=1, inplace=True)
            logging.info("Found _id and dropped")
        logging.info(f"data shape : {df.shape}")

        return df
    
    except Exception as e:
        raise RetailException(e,sys)
    

def write_into_yaml(file_path , data : dict):

    try:
        file_dir=os.path.dirname(file_path)
        os.makedirs(file_dir, exist_ok=True)
        with open(file_path, "w") as write:
            yaml.dump(data, write)

    except Exception as e:
        raise RetailException(e, sys)



def save_numpy_array_data(file_path: str, array: np.array):

    try:
        dir_path = os.path.dirname(file_path)
        os.makedirs(dir_path, exist_ok=True)
        with open(file_path, "wb") as file_obj:
            np.save(file_obj, array)
    except Exception as e:
        raise RetailException(e, sys)
    


def save_object(file_path: str, obj: object) -> None:
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "wb") as file_obj:
            dill.dump(obj, file_obj)
    except Exception as e:
        raise RetailException(e, sys)
    
