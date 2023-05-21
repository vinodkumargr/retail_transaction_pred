from Retail_transcation.logger import logging
from Retail_transcation.exception import RetailException
from Retail_transcation.entity import config_entity, artifacts_entity
from Retail_transcation import config
from Retail_transcation import utils
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
        
    def drop_null_value_columns(self, df:pd.DataFrame, report_key_names:str)->Optional[pd.DataFrame]:
        try:
            
            null_report = df.isna().sum()/df.shape[1]
            drop_column_names = null_report[null_report > 200].index
            self.data_validation_errors[report_key_names] = list(drop_column_names)

            df.drop(list(drop_column_names), axis=1, inplace=True)
            
            logging.info(f"now shape of df : {df.shape}")
            logging.info(f"columns : {df.columns}")

            return df

        except Exception as e:
            raise RetailException(e,sys)
        

    def drop_null_value_rows(self, df:pd.DataFrame, report_key_names:str)->Optional[pd.DataFrame]:
        try:
            
            null_report = df.isna().sum(axis=1)/df.shape[0]
            drop_null_value_column_rows = null_report[null_report > 0.00].index
            self.data_validation_errors[report_key_names] = list(drop_null_value_column_rows.shape)

            df.drop(list(drop_null_value_column_rows), axis=0, inplace=True)
            
            logging.info(f"now shape of df : {df.shape}")
            logging.info(f"columns : {df.columns}")

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
            drop_quantity = df[(df['Quantity'] < 1) | (df['Quantity'] > 40)]
            drop_quantity_shape = drop_quantity.shape

            logging.info(f"total values <1 and >40 in Quantity column is : {drop_quantity_shape}")
            df.drop(drop_quantity.index, axis=0, inplace=True)

            logging.info("dropped Quantity <1 and >40")


            # drop rows for UnitPrice less than 1 and greater than 30
            drop_UnitPrice = df[(df['UnitPrice'] < 1) | (df['UnitPrice'] > 30)]
            drop_UnitPrice_shape = drop_UnitPrice.shape
            
            logging.info(f"total values <1 and >30 in Quantity column is : {drop_UnitPrice_shape}")
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

            logging.info(f"after dropping some rows df shape : {df.shape}")

            logging.info(f"data frame : {df.head(5)}")

            return df

        except Exception as e:
            raise RetailException(e,sys)

        

    def initiate_data_validation(self)->artifacts_entity.DataValidationArtifact:
        try:
            logging.info("data validation started :")

            logging.info("____________________dropping null values, axis=1_________________________")
            base_df = pd.read_csv(self.data_validation_config.base_file_path)
            base_df.replace({"na", np.NAN}, inplace=True)
            logging.info("replaced na with np.NAN")

            logging.info("dropping null values in base_df, axis=1")
            base_df=self.drop_null_value_columns(df=base_df, report_key_names="dropping_missing_value_columns_in_base_df")
            logging.info("dropped null values in base_df")

            logging.info("dropping null values in train and test data,axis-1")
            train_df = pd.read_csv(self.data_ingestion_artifacts.train_file_path)
            train_df=self.drop_null_value_columns(df=train_df, report_key_names="dropping_missing_value_columns_in_train_df")
            test_df = pd.read_csv(self.data_ingestion_artifacts.test_file_path)
            test_df=self.drop_null_value_columns(df=test_df, report_key_names="dropping_missing_value_columns_in_test_df")
            logging.info("dropped null val ues in train and test df")


            logging.info("____________________dropping null values, axis=0_________________________")
            logging.info("dropping null values in base_df, axsi-0")
            base_df = self.drop_null_value_rows(df=base_df,report_key_names="dropping_missing_value_rows_in_base_df")

            logging.info("dropping null values in train and test df, axis-0")
            train_df = self.drop_null_value_rows(df=train_df,report_key_names="dropping_missing_value_rows_in_train_df")
            test_df = self.drop_null_value_rows(df=test_df,report_key_names="dropping_missing_value_rows_in_test_df")
            logging.info("dropped null values, axis=0")


            logging.info("____________________dropping unwanted cols, axis=1_________________________")
            logging.info("dropping null values in base_df, axsi-0")
            base_df = self.drop_unwanted_columns(df=base_df,report_key_names="dropping_unwanted_cols_in_base_df")

            logging.info("dropping null values in train and test df, axis-0")
            train_df = self.drop_unwanted_columns(df=train_df,report_key_names="dropping_unwanted_cols_in_train_df")
            test_df = self.drop_unwanted_columns(df=test_df,report_key_names="dropping_unwanted_cols_in_test_df")
            logging.info("dropped unwanted cols in train and test")


            logging.info("____________________dropping rows on condition(outliers), axis=1_________________________")

            logging.info("dropping some rows based on some conditions")
            base_df = pd.read_csv(self.data_validation_config.base_file_path)

            logging.info("dropping rows on condition in base_df")
            base_df=self.drop_rows_on_condition(df=base_df)
            logging.info("dropped null values in base_df")

            logging.info("dropping rows on condition in train and test data")
            train_df = pd.read_csv(self.data_ingestion_artifacts.train_file_path)
            train_df=self.drop_rows_on_condition(df=train_df)
            test_df = pd.read_csv(self.data_ingestion_artifacts.test_file_path)
            test_df=self.drop_rows_on_condition(df=test_df)
            logging.info("dropped rows on condition in train and test df")


            logging.info("Writting reprt in yaml file")
            utils.write_into_yaml(file_path=self.data_validation_config.report_file_path,
            data=self.data_validation_errors)
            logging.info("report written into report.yaml")


            logging.info("data validation is almost done")
            data_validation_artifact = artifacts_entity.DataValidationArtifact(
                report_file_path=self.data_validation_config.report_file_path)
            logging.info("returning data_validation_artifact")

            

            return data_validation_artifact

        except Exception as e:
            raise RetailException(e,sys)

