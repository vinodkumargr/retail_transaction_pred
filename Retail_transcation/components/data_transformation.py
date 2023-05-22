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
                return df
            
            logging.info('creating columns Day, Month and Year(from 2000) from InvoiceDate')

            df['Day']=df['InvoiceDate'].dt.day
            df['Month']=df['InvoiceDate'].dt.month
            df['Year']=df['InvoiceDate'].dt.year - 2000

            df.drop(['InvoiceDate'],axis=1,inplace=True)

            logging.info("Created new columns Day, Month and year and dropped column InvoiceDate")

            return df

        except Exception as e:
            raise RetailException(e,sys)
        

    def encode_object_columns(self, df:pd.DataFrame):
        try:

            logging.info("encoding object columns ")

            encoder = OneHotEncoder(dtype=int)

            # Apply one-hot encoding on 'Description' and 'country' columns

            Description = encoder.fit_transform(df['Description'].values.reshape(-1,1))
            Country = encoder.fit_transform(df['Country'].values.reshape(-1,1))

            df = pd.concat(df, pd.concat(Description, Country, axis=1), axis=1)

            df = df.drop(columns=['Description','Country'], axis=1)

            return df

        except Exception as e:
            raise RetailException(e, sys)


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