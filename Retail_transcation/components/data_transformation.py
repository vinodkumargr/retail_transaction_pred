from Retail_transcation.exception import RetailException
from Retail_transcation.logger import logging
from Retail_transcation.components.data_ingestion import DataIngestion
from Retail_transcation.components.data_validation import DataValidation
from Retail_transcation import config
from Retail_transcation.entity import config_entity, artifacts_entity
import os, sys, re, pickle
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
            top_n_values = 100
            encoder = OneHotEncoder(sparse=False, handle_unknown='ignore')

            for column in categorical_columns:
                # Get the top N most frequent values in the column and convert into list
                top_values = df[column].value_counts().nlargest(top_n_values).index.tolist()

                # Filter the column to include only the top N values
                filtered_column = df[column].where(df[column].isin(top_values), other='Other')

                encoded_values = encoder.fit_transform(filtered_column.values.reshape(-1, 1))

                encoded_columns = encoder.get_feature_names([column])

                # Create a DataFrame with the encoded values
                encoded_df = pd.DataFrame(encoded_values, columns=encoded_columns)

                encoded_dfs.append(encoded_df)

            f_df = pd.concat([df.drop(categorical_columns, axis=1)] + encoded_dfs, axis=1)


            encoder_file_path = "/home/vinod/projects/retail_transaction_pred/encoder.pkl"
            os.makedirs(os.path.dirname(encoder_file_path), exist_ok=True)
            # Save the encoder as a pickle file
            with open(encoder_file_path, "wb") as f:
                pickle.dump(encoder, f)
                

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