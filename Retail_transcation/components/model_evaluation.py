from Retail_transcation.exception import RetailException
from Retail_transcation.logger import logging
from Retail_transcation.components.data_ingestion import DataIngestion
from Retail_transcation.components.data_validation import DataValidation
from Retail_transcation.components.data_transformation import DataTransformation
from Retail_transcation import config, utils
from Retail_transcation.entity import config_entity, artifacts_entity
from Retail_transcation.predictor import ModelResolver
import os, sys, re
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score



class ModelEvaluation:
    def __init__(self, model_evaluation_config:config_entity.ModeEvaluationConfig,
                 data_ingestion_artifacts:artifacts_entity.DataIngestionArtifact,
                 data_validation_artifacts:artifacts_entity.DataValidationArtifact,
                 data_transformation_artifacts:artifacts_entity.DataTransformationArtifact,
                 model_trainer_artifacts:artifacts_entity.ModelTrainerArtifact):
        
        try:

            self.model_evaluation_config=model_evaluation_config
            self.data_ingestion_artifact = data_ingestion_artifacts
            self.data_validation_artifacts = data_validation_artifacts
            self.data_transformation_artifacts = data_transformation_artifacts
            self.model_trainer_artifacts = model_trainer_artifacts
            self.model_resolver = ModelResolver()

        except Exception as e:
            raise RetailException(e, sys)
        

    def initiate_model_evaluation(self)-> artifacts_entity.ModelEvaluationArtifact:
        try:

            logging.info("Model evaluation started ......")
            latest_dir_path = self.model_resolver.get_latest_dir_path()

            if latest_dir_path == None:  # if the model accuracy is not increased then it will not creates new model dirs 
                model_evaluation_artifact=artifacts_entity.ModelEvaluationArtifact(model_eccepted=True, improved_accuracy=None)

                logging.info(f"model_evaluation_artifact : {model_evaluation_artifact}")

                return model_evaluation_artifact
        

            #find previous/old model location
            logging.info("finding old model path...")
            old_transformer_path = self.model_resolver.get_latest_save_transform_path()
            old_model_path = self.model_resolver.get_latest_model_path()
            
            # read previous model
            logging.info("reading old model...")
            old_transformer = pd.read_csv(old_transformer_path)
            old_model = utils.load_object(file_path=old_model_path)

            # read current/new model
            logging.info("reading new model...")
            current_transformer = pd.read_csv(self.data_transformation_artifacts.feature_store_path)
            current_model = utils.load_object(file_path=self.model_trainer_artifacts.model_path)

            # reading old test data and predicting for old model
            old_test_data = old_transformer
            old_x_test, old_y_test = old_test_data.drop([config.TARGET_COLUMN], axis=1) , old_test_data[config.TARGET_COLUMN]

            old_model_y_pred = old_model.predict(old_x_test)

            #previous model r2_score
            logging.info("comapring models....")
            prevoius_model_r2_score = r2_score(y_true=old_y_test, y_pred=old_model_y_pred)
            logging.info(f"previous model r2_score : {prevoius_model_r2_score}")


            # reading new test data and predicting for new model
            new_x_test_data = current_transformer
            new_x_test, new_y_test = new_x_test_data.drop([config.TARGET_COLUMN], axis=1) , new_x_test_data[config.TARGET_COLUMN]

            new_model_y_pred = current_model.predict(new_x_test)

            current_model_r2_score = r2_score(y_true=new_y_test, y_pred=new_model_y_pred)
            logging.info(f"current_model r2_score : {current_model_r2_score}")

               

            model_evaluation_artifact = artifacts_entity.ModelEvaluationArtifact(
                                                    model_eccepted=True, 
                                                    improved_accuracy=current_model_r2_score - prevoius_model_r2_score)

            return model_evaluation_artifact


        except Exception as e:
            raise RetailException(e, sys)