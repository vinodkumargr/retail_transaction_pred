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
from sklearn.model_selection import train_test_split


class DataTransformation:

    def __init__(self, data_transformation_cofig:config_entity.DataTransformationConfig,
                        data_validation_artifacts:artifacts_entity.DataValidationArtifact):
        try:
            
            self.data_transformation_config=data_transformation_cofig
            self.data_validation_artifacts = data_validation_artifacts

        except Exception as e:
            raise RetailException(e, sys)
        

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
            df['Description'] = df['Description'].apply(lambda x: re.sub(r'\d+', '', x))
            categorical_columns = df.select_dtypes(include='object').columns
            encoded_dfs = []
            top_n_values=200
                    
            for column in categorical_columns:
                # Get the top N most frequent values in the column and convert into list
                top_values = df[column].value_counts().nlargest(top_n_values).index.tolist()

                # Filter the column to include only the top N values
                filtered_column = df[column].where(df[column].isin(top_values), other='Other')

                encoded_df = pd.get_dummies(filtered_column, drop_first=True, dtype=int, dummy_na=False, prefix='', prefix_sep='')
                encoded_dfs.append(encoded_df)
                        
            f_df = pd.concat([df.drop(categorical_columns, axis=1)] + encoded_dfs, axis=1)
            return f_df

        except Exception as e:
            raise RetailException(e, sys)


        
    def convert_dtypes_into_int(self, df:pd.DataFrame):
        try:

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

            df['Total_price'] = df['Quantity'] * df['UnitPrice']

            return df

        except Exception as e:
            raise RetailException(e, sys)
        


    def initiate_data_transformation(self)-> artifacts_entity.DataTransformationArtifact:
        try:

            logging.info("...................Starting data transfomration ..................")

            logging.info("reading data from valid_feature_store_file...")
            base_df = pd.read_csv(self.data_validation_artifacts.valid_feature_store_path)


            # handling INvoiceDate:
            logging.info("handling InvoiceDate column in base_df")
            base_df=self.handle_InvoiceDate(df = base_df)


            # simple imputer
            logging.info(".simple imputer in base_df..")
            base_df=self.impute_missing_values(df=base_df)


            # encode onject columns:
            logging.info(".........encoding objects column in base_df...........")
            base_df=self.encode_object_columns(df=base_df)


            #converting data-types into int
            logging.info(".........converting column.dtypes into int in base_df...........")
            base_df=self.convert_dtypes_into_int(df=base_df)


            # creating target column:
            logging.info(" ....... creating target columns .............")
            base_df=self.create_target_column(df=base_df)


            logging.info (f".......base_df shape ...{base_df.shape}")\


            logging.info("data transformation is almost done.......")


            #saving transformed data into data_transformation artifacts
            logging.info("saving transformed data into data_transformation artifacts")

            base_df.to_csv(path_or_buf=self.data_transformation_config.transform_feature_store_path, index=False, header=True)
            logging.info("saved transfomred base_df into data_transformation artifacts ")

            logging.info("splitting data into train and test.....: ")
            train_df, test_df = train_test_split(base_df, random_state=3)

            logging.info(f"base_df shape is : {base_df.shape}")


            train_df.to_csv(path_or_buf=self.data_transformation_config.transform_train_path, index=False, header=True)
            test_df.to_csv(path_or_buf=self.data_transformation_config.transform_test_path, index=False, header=True)


            data_transformation_Artifact=artifacts_entity.DataTransformationArtifact(
                transform_feature_store_path=self.data_transformation_config.transform_feature_store_path,
                transform_train_path=self.data_transformation_config.transform_train_path,
                transform_test_path=self.data_transformation_config.transform_test_path)

            logging.info("returning data transformatin artifact")
            return data_transformation_Artifact

        except Exception as e:
            raise RetailException(e, sys)