from Retail_transcation.exception import RetailException
from Retail_transcation.logger import logging
from Retail_transcation.components.data_validation import DataValidation
from Retail_transcation.entity import config_entity, artifacts_entity
from Retail_transcation import config, utils
import os, sys, pickle
import pandas as pd
import numpy as np
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer



class DataTransformation:

    def __init__(self, data_transformation_cofig:config_entity.DataTransformationConfig,
                        data_validation_artifacts:artifacts_entity.DataValidationArtifact):
        try:
            
            self.data_transformation_config=data_transformation_cofig
            self.data_validation_artifacts = data_validation_artifacts

        except Exception as e:
            raise RetailException(e, sys)
        

    def get_transform(self):
        try:
            # Select categorical columns
            categorical_columns = ['Description', 'Country']

            # Initialize the OneHotEncoder
            encoder = OneHotEncoder(sparse=False, handle_unknown='ignore')

            # Create a ColumnTransformer for encoding categorical columns
            transformer = ColumnTransformer([
                ('one_hot_encoder', encoder, categorical_columns)
            ], remainder='passthrough')

            return transformer

        except Exception as e:
            raise RetailException(e, sys)

    def initiate_data_transformation(self) -> artifacts_entity.DataTransformationArtifact:
        try:
            logging.info("...................Starting data transformation..................")

            logging.info("reading data from valid_feature_store_file...")
            train_df = pd.read_csv(self.data_validation_artifacts.valid_train_path)
            test_df = pd.read_csv(self.data_validation_artifacts.valid_test_path)

            #split into input and out features
            input_train_features , out_train_feature = train_df.drop(columns=[config.TARGET_COLUMN], axis=1) , train_df[config.TARGET_COLUMN]
            input_test_features , out_test_feature = test_df.drop(columns=[config.TARGET_COLUMN], axis=1) , test_df[config.TARGET_COLUMN]

            # Get the transformer object
            transformer = self.get_transform()

            # Perform the transformation on the train and test data
            input_train_preprocessing_arr = transformer.fit_transform(input_train_features)
            input_test_preprocessing_arr = transformer.transform(input_test_features)

            # combine the input arr and output feature
            train_arr = np.c_[input_train_preprocessing_arr , np.array(out_train_feature)]
            test_arr = np.c_[input_test_preprocessing_arr , np.array(out_test_feature)]

            utils.save_numpy_array_data(file_path=self.data_transformation_config.transform_train_path,
                                        array=train_arr)
            utils.save_numpy_array_data(file_path=self.data_transformation_config.transform_test_path,
                                        array=test_arr)

        
            # Save the pre-processing object
            utils.save_object(file_path=self.data_transformation_config.pre_process_object_path,
                            obj=transformer)


            # save the data from data validation(helps in loading unique values in single prediction)
            data_obj = pd.read_csv(self.data_validation_artifacts.valid_train_path)
            utils.save_object(file_path=self.data_transformation_config.single_pred_data_path,
                              obj=data_obj)



            data_transformation_Artifact=artifacts_entity.DataTransformationArtifact(
                transform_train_path=self.data_transformation_config.transform_train_path,
                transform_test_path=self.data_transformation_config.transform_test_path,
                pre_process_object_path = self.data_transformation_config.pre_process_object_path)

            logging.info("returning data transformatin artifact")
            return data_transformation_Artifact

        except Exception as e:
            raise RetailException(e, sys)