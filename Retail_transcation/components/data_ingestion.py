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

            df = pd.read_csv("/home/vinod/projects/retail_transaction_pred/Online_Retail.csv")
            #df=pd.DataFrame = utils.get_as_df(database_name=self.data_ingestion_config.database_name,
            #                                collection_name=self.data_ingestion_config.collection_name)
            #database_name=self.data_ingestion_config.database_name
            #collection_name=self.data_ingestion_config.collection_name
            logging.info("got data from mongodb")

            x_train, x_test = train_test_split(df, test_size=0.35)

            logging.info("storing train data")
            x_train.to_csv(path_or_buf = self.data_ingestion_config.train_path, index=False, header=True)
            x_test.to_csv(path_or_buf = self.data_ingestion_config.test_path, index=False, header=True)

            
            #preparing artifacts folder:
            data_ingestion_artifact = artifacts_entity.DataIngestionArtifact(
                train_data_path=self.data_ingestion_config.train_path,
                test_data_path=self.data_ingestion_config.test_path)

            return data_ingestion_artifact

        except Exception as e:
            raise RetailException(e,sys)



