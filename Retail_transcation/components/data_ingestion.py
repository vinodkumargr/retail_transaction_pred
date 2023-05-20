import pandas as pd
import numpy as np
import os, sys
from Retail_transcation import utils
from Retail_transcation.exception import RetailException
from Retail_transcation.logger import logging
from Retail_transcation.entity import config_entity, artifacts_entity
from sklearn.model_selection import train_test_split


class DataIngestion:
    
    def __init__(self, data_ingestion_config: config_entity.DataIngestionConfig):
        try:
            self.data_ingestion_config = data_ingestion_config
        except Exception as e:
            raise RetailException(e, sys)
        

    def start_data_ingestion(self)-> artifacts_entity.DataIngestionArtifact:
        try:

            logging.info("Starting data ingestion")
            df=pd.DataFrame = utils.get_as_df(database_name=self.data_ingestion_config.database_name,
                                            collection_name=self.data_ingestion_config.collection_name)
            database_name=self.data_ingestion_config.database_name
            collection_name=self.data_ingestion_config.collection_name
            logging.info("got data from mongodb")

            logging.info("preparing for replace na with NAN")
            df.replace(to_replace = "na", value=np.NAN, inplace=True)

            logging.info("creating feature store dir")
            feature_Store_dir = os.path.dirname(self.data_ingestion_config.feature_store_path)
            os.makedirs(feature_Store_dir,exist_ok=True)

            logging.info("storing into feature store dir")
            df.to_csv(path_or_buf = self.data_ingestion_config.feature_store_path, index=False, header=True)
            
            logging.info("spliting data into train and test")
            train_df, test_df = train_test_split(df, random_state=2)
            
            logging.info("create dataset_dir if not exist")
            dataset_dir = os.path.dirname(self.data_ingestion_config.train_file_path)
            os.makedirs(dataset_dir, exist_ok=True)
            
            logging.info("save dataset to feature store after split")
            train_df.to_csv(path_or_buf=self.data_ingestion_config.train_file_path, index=False, header=True)
            test_df.to_csv(path_or_buf=self.data_ingestion_config.test_file_path, index=False, header=True)
            
            #preparing artifacts folder:
            data_ingestion_artifact = artifacts_entity.DataIngestionArtifact(
                feature_store_path=self.data_ingestion_config.feature_store_path,
                train_file_path=self.data_ingestion_config.train_file_path,
                test_file_path=self.data_ingestion_config.test_file_path)

            return data_ingestion_artifact

        except Exception as e:
            print(e,sys)



