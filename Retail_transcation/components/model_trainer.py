from Retail_transcation.exception import RetailException
from Retail_transcation.logger import logging
from Retail_transcation.components.data_ingestion import DataIngestion
from Retail_transcation.components.data_validation import DataValidation
from Retail_transcation.components.data_transformation import DataTransformation
from Retail_transcation import config, utils
from Retail_transcation.entity import config_entity, artifacts_entity
import os, sys, re
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score


class ModelTrainer:

    def __init__(self, model_trainer_config:config_entity.ModeTrainerConfig,
                    data_transformation_artifacts:artifacts_entity.DataTransformationArtifact):
        try:
            
            self.model_trainer_config = model_trainer_config
            self.data_transformation_artifacts = data_transformation_artifacts

        except Exception as e:
            raise RetailException(e, sys)
        

    def linear_regression_algorith(self, x_train, y_train):
        try:

            lr = LinearRegression()
            lr.fit(x_train, y_train)
            return lr

        except Exception as e:
            raise RetailException(e, sys)
        

    def initiate_model_trainer(self)-> artifacts_entity.ModelTrainerArtifact:
        try:

            logging.info("Reading traina nd test data from data_transformation artifacts...")
            train_df = pd.read_csv(self.data_transformation_artifacts.transform_train_path)
            test_df = pd.read_csv(self.data_transformation_artifacts.transform_test_path)

            logging.info("splitting train and test data into x_train, x_test and y_train, y_test ...")
            x_train, y_train = train_df.drop(config.TARGET_COLUMN, axis=1) , train_df[config.TARGET_COLUMN]
            x_test, y_test = test_df.drop(config.TARGET_COLUMN, axis=1) , test_df[config.TARGET_COLUMN]

            logging.info("ready to fit tdata to model...")
            model = self.linear_regression_algorith(x_train=x_train, y_train=y_train)
            logging.info("data fitted to model...")

            logging.info("predict for x_test...")
            y_pred_test = model.predict(x_test)
            r2_test_score = r2_score(y_true=y_test, y_pred=y_pred_test)
            logging.info("predicted for x_test")


            logging.info("predict for x_train...")
            y_pred_train = model.predict(x_train)
            r2_train_score = r2_score(y_true=y_train, y_pred=y_pred_train)
            logging.info("predicted for x_train")

            logging.info("calculating absolute diff between r2_train_score and r2_test_score")
            diff=abs(r2_train_score - r2_test_score)


            logging.info("observing model performance whether is underfitted or overfitted...")
            if r2_test_score < self.model_trainer_config.expected_r2_score:
                raise Exception(f"model expected r2_score is : {self.model_trainer_config.expected_r2_score}, but got model r2_score is : {r2_test_score}")
            elif diff > self.model_trainer_config.overfitting_value:
                raise Exception(f"the absolute diff between train and test r2_score is : {diff} : model is overfitted...")
            print(f"accuracy is good, no underfitting, no overfitting")


            logging.info("save the model as a dill(pickle) file....")
            utils.save_object(file_path=self.model_trainer_config.model_path, 
                                obj=model)
            
            
            model_trainer_artifacts = artifacts_entity.ModelTrainerArtifact(
                model_path=self.model_trainer_config.model_path,
                r2_train_score=r2_train_score,
                r2_test_score=r2_test_score)


        except Exception as e:
            raise RetailException(e,sys)








