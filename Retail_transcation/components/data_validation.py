from Retail_transcation.logger import logging
from Retail_transcation.exception import RetailException
from Retail_transcation.entity import config_entity, artifacts_entity
from Retail_transcation import config
import pandas as pd
import numpy as np
import os, sys
import datetime
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
        

    def drop_rows_on_condition(self,df:pd.DataFrame):
        try:

            # drop rows for quantity less than 1 and greater than 40
            logging.info("checking quantity <1 and >40")
            drop_quantity = df[(df['Quantity'] < 1) | (df['Quantity'] > 40)]
            drop_quantity_shape = drop_quantity.shape

            logging.info(f"total values <1 and >40 in Quantity column is : {drop_quantity_shape}")
            logging.info("dropping Quantity <1 and >40")
            df.drop(drop_quantity.index, axis=0, inplace=True)

            logging.info("dropped Quantity <1 and >40")


            # drop rows for UnitPrice less than 1 and greater than 30
            logging.info("checking UnitPrice <1 and >30")
            drop_UnitPrice = df[(df['UnitPrice'] < 1) | (df['UnitPrice'] > 30)]
            drop_UnitPrice_shape = drop_UnitPrice.shape
            
            logging.info(f"total values <1 and >30 in Quantity column is : {drop_UnitPrice_shape}")
            logging.info("dropping UnitPrice <1 and >30")
            df.drop(drop_UnitPrice.index, axis=0, inplace=True)
            logging.info("dropped UnitPrice <1 and >30")

            df["UnitPrice"] = df['UnitPrice'].astype(int)
            logging.info("converted UnitPrice dtype into 'int' after dropping <1 and >30 values")


            # drop rows for Country <500
            drop_country = df['Country'].value_counts()[df['Country'].value_counts() <500]
            logging.info(f" Total 'Country' values_count less than 500 : {drop_country[0]}")
            drop_country_index=drop_country.index

            logging.info("Dropping country value_counts <500")
            df.drop(df[df['Country'].isin(drop_country_index)].index, axis=0, inplace=True)

            logging.info("Dropped country value_counts <500")


            # drop rows for Description < 300
            logging.info("strip Description")
            df['Description']=df['Description'].str.strip()

            drop_Description = df['Description'].value_counts()[df['Description'].value_counts() < 300]
            logging.info(f" Total 'Description' values_count < 300 : {drop_Description[0]}")
            drop_Description_index=drop_country.index

            logging.info("Dropping Description value_counts <300")
            df.drop(df[df['Description'].isin(drop_Description_index)].index, axis=0, inplace=True)

            logging.info("Dropped Description value_counts < 300")

            return df

        except Exception as e:
            raise RetailException(e,sys)



    def handle_InvoiceDate(self, df:pd.DataFrame):
        try:
            logging.info("checking InvoiceDate :")
            if df['InvoiceDate'].dtype !='datetime':
                logging.info('df[InvoiceDate] dtype != datetime, converting into datetime')
                df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
            else:
                return df
            
            logging.info('creating columns Day, Month and Year from InvoiceDate')

            df['Day']=df['InvoiceDate'].dt.day
            df['Month']=df['InvoiceDate'].dt.month
            df['Year']=df['InvoiceDate'].dt.year - 2000

            df.drop(['InvoiceDate'],axis=1,inplace=True)

            logging.info("Created new columns Day, Month and year and dropped column InvoiceDate")

        except Exception as e:
            raise RetailException(e,sys)
        

    def initiate_data_validation(self)->artifacts_entity.DataValidationArtifact:
        try:
            logging.info("data validation started :")

            base_df = pd.read_csv(self.data_validation_config.base_file_path)
            base_df.replace({"na", np.NAN}, inplace=True)
            logging.info("replaced na with np.NAN")

            logging.info("dropping null values in base_df")
            base_df=self.drop_null_values(df=base_df, report_key_names="dropping_missing_value_columns_in_base_df")
            logging.info("dropped null values in base_df")


        except Exception as e:
            raise RetailException(e,sys)

