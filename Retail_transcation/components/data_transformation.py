from Retail_transcation.exception import RetailException
from Retail_transcation.logger import logging
from Retail_transcation.components import data_ingestion, data_validation
from data_validation import DataValidation
from data_ingestion import DataIngestion
from Retail_transcation import config
from Retail_transcation.entity import config_entity, artifacts_entity
import os, sys, re
import pandas as pd
import numpy as np


class DataTransformation:

    def __init__(self, data_transformation_cofig:config_entity.DataTransformationConfig,
                        data_ingestion_artifacts:artifacts_entity.DataIngestionArtifact,
                        data_validation_artifacts:artifacts_entity.DataValidationArtifact):
        try:
            
            self.data_transformation_config=data_transformation_cofig
            self.data_ingestion_artifacts = data_ingestion_artifacts
            self.data_validation_artifacts = data_validation_artifacts

        except Exception as e:
            raise RetailException(e, sys)
        

    def handle_InvoiceDate(self, df:pd.DataFrame):
        try:
            logging.info("checking InvoiceDate :")
            if df['InvoiceDate'].dtype !='datetime':
                logging.info('df[InvoiceDate] dtype != datetime, converting into datetime')
                df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
            else:
                return df
            
            logging.info('creating columns Day, Month and Year(from 2000) from InvoiceDate')

            df['Day']=df['InvoiceDate'].dt.day
            df['Month']=df['InvoiceDate'].dt.month
            df['Year']=df['InvoiceDate'].dt.year - 2000

            df.drop(['InvoiceDate'],axis=1,inplace=True)

            logging.info("Created new columns Day, Month and year and dropped column InvoiceDate")

        except Exception as e:
            raise RetailException(e,sys)
        

    def create_target_column(self, df:pd.DataFrame):
        try:

            logging.info('creating target column')

            df['Total_price'] = df['Quantity'] * df['UnitPrice']
            logging.info('Created target column -> Total_price')

        except Exception as e:
            raise RetailException(e, sys)
        

    def encode_object_columns(self, df:pd.DataFrame):
        try:

            logging.info("encoding object columns ")

            # Preprocessing the 'Description' column to remove numerical values
            df['Description'] = df['Description'].apply(lambda x: re.sub(r'\d+', '', x))

            df=pd.get_dummies(df, columns=config.ENCODING_COLUMNS, 
                                    drop_first=True,dtype=int, dummy_na=False, prefix='', prefix_sep='')

        except Exception as e:
            raise RetailException(e, sys)
        
    def convert_dtypes_into_int(self, df:pd.DataFrame):
        try:

            logging.info("converting float columns into integer")

            for column in df.columns:
                df[column] = df[column].astype(int)
            

        except Exception as e:
            raise RetailException(e, sys)