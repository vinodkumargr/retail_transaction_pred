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



    def drop_null_value_rows(self, df:pd.DataFrame, report_key_names:str)->Optional[pd.DataFrame]:
        try:
            
            null_report = df.isnull().sum(axis=1)/df.shape[0]
            drop_null_value_column_rows = null_report[null_report > 0.00].index
            
            self.data_validation_errors[report_key_names] = list(drop_null_value_column_rows.shape)

            df = df.drop(list(drop_null_value_column_rows), axis=0)
            df = df.dropna(axis=0)
            
            logging.info(f"now shape of df : {df.shape}")
            logging.info(f"columns : {df.columns}")

            return df

        except Exception as e:
            raise RetailException(e,sys)

    def drop_unwanted_columns(self, df: pd.DataFrame, report_key_names: str) -> pd.DataFrame:
        try:
            columns = config.UNWANTED_COLUMNS
            found_unwanted_columns = [column for column in df.columns if column in columns]
            logging.info(f"found unwanted columns: {found_unwanted_columns}")

            self.data_validation_errors[report_key_names] = list(found_unwanted_columns)

            df = df.drop(columns=found_unwanted_columns, axis=1)
            logging.info(f"dropped unwanted columns: {found_unwanted_columns}")

            logging.info(f"data shape: {df.shape}")

            return df

        except Exception as e:
            raise RetailException(e, sys)


        except Exception as e:
            raise RetailException(e, sys)

        

    def drop_rows_on_condition(self,df:pd.DataFrame):
        try:


            # drop rows for quantity less than 1 and greater than 40
            drop_quantity = df[(df['Quantity'] < 1) | (df['Quantity'] > 40)]
            drop_quantity_shape = drop_quantity.shape

            df = df.drop(drop_quantity.index, axis=0)



            # drop rows for UnitPrice less than 1 and greater than 30
            drop_UnitPrice = df[(df['UnitPrice'] < 1) | (df['UnitPrice'] > 30)]
            df = df.drop(drop_UnitPrice.index, axis=0)

            df["UnitPrice"] = df['UnitPrice'].astype(int)



            # drop rows for Country <500
            drop_country = df['Country'].value_counts()[df['Country'].value_counts() <500]
            drop_country_index=drop_country.index

            df = df.drop(df[df['Country'].isin(drop_country_index)].index, axis=0)



            # drop rows for Description < 300
            df['Description']=df['Description'].str.strip()
            drop_Description = df['Description'].value_counts()[df['Description'].value_counts() < 300]
            drop_Description_index=drop_Description.index

            df = df.drop(df[df['Description'].isin(drop_Description_index)].index, axis=0)

            return df

        except Exception as e:
            raise RetailException(e,sys)

        

    def initiate_data_validation(self)->artifacts_entity.DataValidationArtifact:
        try:
            logging.info("data validation started........")

            base_df = pd.read_csv(self.data_validation_config.base_file_path)
            train_df = pd.read_csv(self.data_ingestion_artifacts.train_file_path)
            test_df = pd.read_csv(self.data_ingestion_artifacts.test_file_path)


            logging.info(f"dropping null values in base_df, axsi-0 and df shape is : {base_df.shape}")
            base_df = self.drop_null_value_rows(df=base_df,report_key_names="dropping_missing_value_rows_in_base_df")

            logging.info("dropping null values in train and test df, axis-0")
            train_df = self.drop_null_value_rows(df=train_df,report_key_names="dropping_missing_value_rows_in_train_df")
            test_df = self.drop_null_value_rows(df=test_df,report_key_names="dropping_missing_value_rows_in_test_df")
            logging.info(f"dropped null values, axis=0 train_df .shape = {train_df.shape} , test_df.shape = {test_df.shape}")


            logging.info("____________________dropping unwanted cols, axis=1_________________________")
            logging.info(f"dropping null values in base_df, axsi-0 and df shape is : {base_df.shape}")
            base_df = self.drop_unwanted_columns(df=base_df,report_key_names="dropping_unwanted_cols_in_base_df")

            logging.info("dropping null values in train and test df, axis-0")
            train_df = self.drop_unwanted_columns(df=train_df,report_key_names="dropping_unwanted_cols_in_train_df")
            test_df = self.drop_unwanted_columns(df=test_df,report_key_names="dropping_unwanted_cols_in_test_df")
            logging.info(f"dropped unwanted cols in train and test train_df .shape = {train_df.shape} , test_df.shape = {test_df.shape}")


            logging.info("____________________dropping rows on condition(outliers), axis=1_________________________")

            logging.info(f"dropping rows on condition in base_df and df shape is : {base_df.shape}")
            base_df=self.drop_rows_on_condition(df=base_df)
            logging.info("dropped null values in base_df")

            logging.info("dropping rows on condition in train and test data")
            train_df=self.drop_rows_on_condition(df=train_df)
            test_df=self.drop_rows_on_condition(df=test_df)
            logging.info(f"dropped rows on condition in train and test df train_df .shape = {train_df.shape} , test_df.shape = {test_df.shape}")


            logging.info("Writting reprt in yaml file")
            utils.write_into_yaml(file_path=self.data_validation_config.report_file_path,
            data=self.data_validation_errors)
            logging.info("report written into report.yaml")

            logging.info("storing validation base_df into data validation artifacts")
            base_df.to_csv(path_or_buf=self.data_validation_config.valid_feature_store_path, index=False, header=True)

            logging.info("storing validation train data into data validation artifacts")
            train_df.to_csv(path_or_buf=self.data_validation_config.valid_train_file_path, index=False, header=True)

            logging.info("storing validation test data into data validation artifacts")
            test_df.to_csv(path_or_buf=self.data_validation_config.valid_test_file_path, index=False, header=True)

            logging.info("data validation is almost done")

            data_validation_artifact = artifacts_entity.DataValidationArtifact(
                report_file_path=self.data_validation_config.report_file_path,
                valid_feature_store_path=self.data_validation_config.valid_feature_store_path,
                valid_train_file_path=self.data_validation_config.valid_train_file_path,
                valid_test_file_path=self.data_validation_config.valid_test_file_path)
            logging.info("returning data_validation_artifact")

            

            return data_validation_artifact

        except Exception as e:
            raise RetailException(e,sys)

