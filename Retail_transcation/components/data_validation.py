from Retail_transcation.logger import logging
from Retail_transcation.exception import RetailException
from Retail_transcation.entity import config_entity, artifacts_entity
from Retail_transcation import config
import pandas as pd
import numpy as np
import os, sys
from typing import Optional


class DataValidation:

    def __init__(self, data_validation_config:config_entity.DataValidationConfig,
                    data_ingestion_artifacts:artifacts_entity.DataIngestionArtifact):
        try:
            
            self.data_validation_config=data_validation_config
            self.data_ingestion_artifacts=data_ingestion_artifacts
            self.data_validation_errors=dict()

        except Exception as e:
            raise RetailException(e,sys)
        

    def drop_null_values(self, df:pd.DataFrame, report_key_names:str)->Optional[pd.DataFrame]:
        try:
            
            null_report = df.isna().sum()/df.shape[1]
            drop_column_names = null_report[null_report > 180].index
            logging.info(f"null values columns : {drop_column_names > 180}")
            logging.info(f"columns to drop : {list(drop_column_names)}")
            self.data_validation_errors[report_key_names] = list(drop_column_names)

            df.drop(list(drop_column_names), axis=1, inplace=True)
            

            if len(drop_column_names == 0):
                logging.info("all_null_value_columns dropped")

            if len(df.columns == 0):
                logging.info("no columns left, all are dropped")
            
            logging.info(f"now shape of df : {df.shape}")
            return df

        except Exception as e:
            raise RetailException(e,sys)
        

    def drop_unwanted_columns(self, df:pd.DataFrame, report_key_names:str):
        try:

            columns = config.UNWANTED_COLUMNS
            found_unwanted_columns = [column for column in columns if column in df.columns]
            logging.info(f"found unwanted columns {columns}")

            self.data_validation_errors[report_key_names] = list(found_unwanted_columns)

            df.drop(found_unwanted_columns,axis=1, inplace=True)
            logging.info(f"dropped unwanted coluumns : {found_unwanted_columns}")

            logging.info(f"data shape : {df.shape}")

            return df

        except Exception as e:
            raise RetailException(e,sys)
        

    def drop_quantity_lt_0(self,df:pd.DataFrame, report_key_name:str):
        try:

            logging.info("checking quantity is less than 1")

            quantity_lt_1 = (df['Quantity']).sum()
            logging.info(f"total values less than 1 in Quantity column is : {quantity_lt_1}")

            logging.info("dropping Quantity less than 1")
            df.drop(df[df['Quantity'] < 1].index, axis=0, inplace=True)

            logging.info("dropped Quantity less than 1")

            return df

        except Exception as e:
            raise RetailException(e,sys)

