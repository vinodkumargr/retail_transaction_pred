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

        except Exception as e:
            raise RetailException(e, sys)