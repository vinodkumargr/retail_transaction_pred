from Retail_transcation.exception import RetailException
from Retail_transcation.logger import logging
from Retail_transcation.components.data_ingestion import DataIngestion
from Retail_transcation.components.data_validation import DataValidation
from Retail_transcation import config
from Retail_transcation.entity import config_entity, artifacts_entity
import os, sys, re
import pandas as pd
import numpy as np
from sklearn.preprocessing import OneHotEncoder
from sklearn.feature_extraction import FeatureHasher
from sklearn.impute import SimpleImputer
import random



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
            if df['InvoiceDate'].dtype !='datetime':
                logging.info('df[InvoiceDate] dtype != datetime, converting into datetime')
                df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
            else:
                pass
            
            logging.info('creating columns Day, Month and Year(from 2000) from InvoiceDate')

            df['Day']=df['InvoiceDate'].dt.day
            df['Month']=df['InvoiceDate'].dt.month
            df['Year']=df['InvoiceDate'].dt.year - 2000

            df = df.drop(['InvoiceDate'],axis=1)

            logging.info("Created new columns Day, Month and year and dropped column InvoiceDate")

            return df

        except Exception as e:
            raise RetailException(e,sys)
        

    def impute_missing_values(self, df:pd.DataFrame):
        try:
            imputer = SimpleImputer(strategy="most_frequent")  
            df_imputed = df.copy()

            for column in df.columns:
                if df[column].isnull().sum() > 0:
                    column_values = df[column].values.reshape(-1, 1)
                    imputed_values = imputer.fit_transform(column_values)
                    df_imputed[column] = imputed_values

            return df_imputed
        
        except Exception as e:
            raise RetailException(e, sys)

    def encode_object_columns(self, df: pd.DataFrame):
        try:
            max_features_description = 380  # Maximum number of hashed features for Description column
            max_features_country = 14  # Number of columns to keep for Country column
            columns = ['Description', 'Country']

            hasher_description = FeatureHasher(n_features=max_features_description, input_type='string')
            hasher_country = FeatureHasher(n_features=max_features_country, input_type='string')

            hashed_dfs = []

            for column in columns:
                if column == 'Description':
                    column_values = df[column].astype(str).values
                    random.shuffle(column_values)  # Randomly shuffle the values
                    hashed_features = hasher_description.transform(column_values[:max_features_description])
                elif column == 'Country':
                    hashed_features = hasher_country.transform(df[column].astype(str))

                hashed_df = pd.DataFrame(hashed_features.toarray())
                hashed_df.columns = [f'{column}_{i}' for i in range(hashed_df.shape[1])]
                hashed_dfs.append(hashed_df)

            # Concatenate the hashed features DataFrames with the original DataFrame
            df = pd.concat(hashed_dfs, axis=1)

            return df

        except Exception as e:
            raise RetailException(e, sys)


        
    def convert_dtypes_into_int(self, df:pd.DataFrame):
        try:

            logging.info("converting float columns into integer")

            for column in df.columns:
                if column in ['Day', 'Month', 'Year']:
                    continue
                elif df[column].dtype != int:
                    df[column] = df[column].astype(int)

            return df

        except Exception as e:
            raise RetailException(e, sys)
        

    def create_target_column(self, df:pd.DataFrame): 
        try:

            logging.info('creating target column')

            df['Total_price'] = df['Quantity'] * df['UnitPrice']
            logging.info('Created target column -> Total_price')

            return df

        except Exception as e:
            raise RetailException(e, sys)
        


    def initiate_data_transformation(self)-> artifacts_entity.DataTransformationArtifact:
        try:

            logging.info("...................Starting data transfomration ..................")

            logging.info("reading data from valid_feature_store_file...")
            base_df = pd.read_csv(self.data_validation_artifacts.valid_feature_store_path)

            logging.info('reading data from valid_train_file.....')
            train_df = pd.read_csv(self.data_validation_artifacts.valid_train_file_path)

            logging.info('reading data from valid_test_file.....')
            test_df = pd.read_csv(self.data_validation_artifacts.valid_test_file_path)


            # handling INvoiceDate:
            logging.info(".........handling InvoiceDate column in base_df...........")
            base_df=self.handle_InvoiceDate(df = base_df)

            logging.info("handling InvoiceDate in train and test data")
            train_df=self.handle_InvoiceDate(df=train_df)
            test_df=self.handle_InvoiceDate(df=test_df)
            logging.info("handled InvoiceDate.......")


            logging.info (f"......................................................................base_df shape ..................................{base_df.shape}")
            logging.info (f"......................................................................train_df shape ..................................{train_df.shape}")
            logging.info (f"......................................................................test_df shape ..................................{test_df.shape}")

            logging.info(f"columns = {base_df.head(5)}")


            # simple imputer
            logging.info(".........simple imputer in base_df...........")
            base_df=self.impute_missing_values(df=base_df)

            logging.info("handling simple imputer in train and test data")
            train_df=self.impute_missing_values(df=train_df)
            test_df=self.impute_missing_values(df=test_df)
            logging.info("handled simple imputer.......")


            # encode onject columns:

            logging.info(".........encoding objects column in base_df...........")
            base_df=self.encode_object_columns(df=base_df)

            logging.info("handling encoding in train and test data")
            train_df=self.encode_object_columns(df=train_df)
            test_df=self.encode_object_columns(df=test_df)
            logging.info("handled encoding.......")


            #converting data-types into int
            logging.info(".........converting column.dtypes into int in base_df...........")
            base_df=self.convert_dtypes_into_int(df=base_df)

            logging.info("converting column.dtypes into int in train and test data")
            train_df=self.convert_dtypes_into_int(df=train_df)
            test_df=self.convert_dtypes_into_int(df=test_df)
            logging.info("converted column.dtypes into int.......")


            # creating target column:
            logging.info(" ....... creating target columns .............")
            base_df=self.create_target_column(df=base_df)

            logging.info("handling target column in train and test data")
            train_df=self.create_target_column(df=train_df)
            test_df=self.create_target_column(df=test_df)
            logging.info("handled target column .......")


            df=pd.get_dummies(df, columns=config.ENCODING_COLUMNS, 
                                    drop_first=True,dtype=int, dummy_na=False, prefix='', prefix_sep='')

            logging.info(f"base_df.head : {base_df.head(3)}")
            logging.info(f"train_df.head : {train_df.head(3)}")
            logging.info(f"test_df.head : {test_df.head(3)}")


            logging.info("data transformation is almost done.....")

            data_transformation_Artifact=artifacts_entity.DataTransformationArtifact(
                transform_feature_store_path=self.data_transformation_config.transform_feature_store_path,
                transform_train_path=self.data_transformation_config.transform_train_file_path,
                transform_test_path=self.data_transformation_config.transform_test_file_path
            )

            logging.info("returning data transformatin artifact")
            return data_transformation_Artifact

        except Exception as e:
            raise RetailException(e, sys)