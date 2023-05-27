from Retail_transcation.logger import logging
from Retail_transcation.exception import RetailException
from Retail_transcation.entity import config_entity, artifacts_entity
from Retail_transcation import config
from Retail_transcation import utils
import pandas as pd
import numpy as np
import os, sys, re
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
            
            #null_report = df.isnull().sum(axis=1)/df.shape[0]
            #drop_null_value_column_rows = null_report[null_report > 0.00].index
            
            #self.data_validation_errors[report_key_names] = list(drop_null_value_column_rows.shape)

            #df = df.drop(list(drop_null_value_column_rows), axis=0)
            df = df.dropna(axis=0)
            
            logging.info(f"now shape of df : {df.shape}")

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



    def drop_rows_on_condition(self,df:pd.DataFrame):
        try:

            # drop rows for quantity less than 1 and greater than 40
            drop_quantity = df[(df['Quantity'] < 1) | (df['Quantity'] > 40)]
            df = df.drop(drop_quantity.index, axis=0)


            # drop rows for UnitPrice less than 1 and greater than 30
            drop_UnitPrice = df[(df['UnitPrice'] < 1) | (df['UnitPrice'] > 30)]
            df = df.drop(drop_UnitPrice.index, axis=0)

            df["UnitPrice"] = df['UnitPrice'].astype(int)
            

            # drop rows for Description < 300
            df['Description']=df['Description'].str.strip()
            drop_Description = df['Description'].value_counts()[df['Description'].value_counts() < 300]
            drop_Description_index=drop_Description.index

            df = df.drop(df[df['Description'].isin(drop_Description_index)].index, axis=0)


            return df

        except Exception as e:
            raise RetailException(e,sys)
        


    def handle_InvoiceDate(self, df:pd.DataFrame):
        try:
            if df['InvoiceDate'].dtype !='datetime':
                logging.info(f'df[InvoiceDate] dtype != datetime, converting into datetime in {df}')
                df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
            else:
                pass


            df['Day']=df['InvoiceDate'].dt.day
            df['Month']=df['InvoiceDate'].dt.month
            df['Year']=df['InvoiceDate'].dt.year - 2000

            df = df.drop(['InvoiceDate'],axis=1)

            logging.info(f"Created new columns Day, Month and year and dropped column InvoiceDate in {df}")

            return df

        except Exception as e:
            raise RetailException(e,sys)


        

    def handle_InvoiceDate(self, df:pd.DataFrame):
        try:
            if df['InvoiceDate'].dtype !='datetime':
                logging.info(f'df[InvoiceDate] dtype != datetime, converting into datetime in {df}')
                df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
            else:
                pass


            df['Day']=df['InvoiceDate'].dt.day
            df['Month']=df['InvoiceDate'].dt.month
            df['Year']=df['InvoiceDate'].dt.year - 2000

            df = df.drop(['InvoiceDate'],axis=1)

            date_col = ['Day', 'Month', 'Year']
            for i in date_col:
                df[i] = df[i].astype(int)

            logging.info(f"Created new columns Day, Month and year and dropped column InvoiceDate")

            return df

        except Exception as e:
            raise RetailException(e,sys)
        
    
    def convert_dtypes_into_int(self, df:pd.DataFrame):
        try:

            for column in df.columns:
                if column in ['Quantity', 'UnitPrice']:
                    df[column] = df[column].astype(int)                    
            return df

        except Exception as e:
            raise RetailException(e, sys)
        

    def create_target_column(self, df:pd.DataFrame): 
        try:

            df['Total_price'] = df['Quantity'] * df['UnitPrice']

            return df

        except Exception as e:
            raise RetailException(e, sys)
        

    def get_required_description(self, df: pd.DataFrame):
        try:
            
            #df['Description'] = df['Description'].apply(lambda x: re.sub(r'\d+', '', x))
            top_n_values = 200
                # Get the top N most frequent values in the column and convert into list
            top_values = df['Description'].value_counts().nlargest(top_n_values).index.tolist()

            # Filter the column to include only the top N values
            filtered_column = df['Description'].where(df['Description'].isin(top_values), other='Others')

            df['Description'] = filtered_column.values

            return df

        except Exception as e:
            raise RetailException(e, sys)


        

    def initiate_data_validation(self)->artifacts_entity.DataValidationArtifact:
        try:
            logging.info("data validation started........")

            train_df = pd.read_csv(self.data_ingestion_artifacts.train_data_path)
            test_df = pd.read_csv(self.data_ingestion_artifacts.test_data_path)

            logging.info(f"dropping null values")
            train_df = self.drop_null_value_rows(df=train_df,report_key_names="dropping_missing_value_rows_in_train_df")
            test_df = self.drop_null_value_rows(df=test_df,report_key_names="dropping_missing_value_rows_in_test_df")


            logging.info(f"dropping null values")
            train_df = self.drop_unwanted_columns(df=train_df,report_key_names="dropping_unwanted_cols_in_train_df")
            test_df = self.drop_unwanted_columns(df=test_df,report_key_names="dropping_unwanted_cols_in_test_df")
        

            logging.info(f"dropping rows on condition")
            train_df=self.drop_rows_on_condition(df=train_df)
            test_df=self.drop_rows_on_condition(df=test_df)


            # handling INvoiceDate:
            logging.info("handling InvoiceDate column")
            train_df=self.handle_InvoiceDate(df = train_df)
            test_df=self.handle_InvoiceDate(df = test_df)


            #converting data-types into int
            logging.info("converting column.dtypes into int.")
            train_df=self.convert_dtypes_into_int(df=train_df)
            test_df=self.convert_dtypes_into_int(df=test_df)

            #logging.info(f"{train_df.head(3)}")
            #logging.info(f"{test_df.head(3)}")


            # creating target column:
            logging.info(" ....... creating target columns .............")
            train_df=self.create_target_column(df=train_df)
            test_df=self.create_target_column(df=test_df)

            # get_required_description
            logging.info(f"get_required_description ")
            train_df=self.get_required_description(df=train_df)
            test_df=self.get_required_description(df=test_df)


            logging.info(f"train_Df  shape : {train_df.shape}")
            logging.info(f"test_df  shape : {test_df.shape}")


            train_df.to_csv(path_or_buf=self.data_validation_config.valid_train_path, index=False, header=True)
            test_df.to_csv(path_or_buf=self.data_validation_config.valid_test_path, index=False, header=True)


            # handling INvoiceDate:
            logging.info("handling InvoiceDate column in base_df")
            base_df=self.handle_InvoiceDate(df = base_df)

            
            logging.info("Writting reprt in yaml file")
            utils.write_into_yaml(file_path=self.data_validation_config.report_file_path,
            data=self.data_validation_errors)
            logging.info("report written into report.yaml")

            logging.info("data validation is almost done")

            data_validation_artifact = artifacts_entity.DataValidationArtifact(
                valid_train_path = self.data_validation_config.valid_train_path,
                valid_test_path= self.data_validation_config.valid_test_path,
                report_file_path=self.data_validation_config.report_file_path)
            
            logging.info("returning data_validation_artifact")


            return data_validation_artifact

        except Exception as e:
            raise RetailException(e,sys)

