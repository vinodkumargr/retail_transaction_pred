from Retail_transcation.exception import RetailException
from Retail_transcation.logger import logging
from Retail_transcation.components.data_ingestion import DataIngestion
from Retail_transcation.components.data_validation import DataValidation
from Retail_transcation.components.data_transformation import DataTransformation
from Retail_transcation.components.model_trainer import ModelTrainer
from Retail_transcation import config, utils
from Retail_transcation.entity import config_entity, artifacts_entity

import os, sys, re




# to train new model, if new data comes, and store the new model.

class ModelResolver:

    def __init__(self, model_registry:str="saved_models",
                    transformer_dir_name="transformer_data",
                    model_dir_name="model"):
        
        try:
            self.model_registry = model_registry
            os.makedirs(self.model_registry, exist_ok=True)
            self.transformer_dir_name = transformer_dir_name
            self.model_dir_name = model_dir_name

        except Exception as e:
            raise RetailException(e, sys)
        

    def get_latest_dir_path(self):
        try:
            
            dir_name=os.path.listdir(self.model_registry)
            if len(dir_name)==0:
                return None
            
            dir_name = list(map(int, dir_name))
            latest_dir_name=max(dir_name)
            
            return os.path.join(self.model_registry, f"{latest_dir_name}")

        except Exception as e:
            raise RetailException(e, sys)
        

        
    def get_latest_model_path(self):
        try:
            
            latest_dir = self.get_latest_dir_path()
            if latest_dir == None:
                raise Exception("model is not available....")
            
            return os.path.join(latest_dir, self.model_dir_name, config_entity.MODEL_FILE_NAME)

        except Exception as e:
            raise RetailException(e, sys)
        

    def get_latest_transformer_path(self):
        try:
            
            latest_dir=self.get_latest_dir_path
            if latest_dir == None:
                raise Exception("transform data is not available....")
            
            return os.path.join(latest_dir, self.transformer_dir_name,config_entity.TRANSFORMATION_BASE_FILE_NAME )

        except Exception as e:
            raise RetailException(e, sys)
        


    def get_latest_save_dir_path(self):
        try:
            
            latest_dir=self.get_latest_dir_path
            if latest_dir == None:
                return os.path.join(self.model_registry,f"{0}")
            
            latest_dir_num=int(os.path.basename(self.get_latest_dir_path()))
            return os.path.join(self.model_registry, f"{latest_dir_num + 1}")

        except Exception as e:
            raise RetailException(e, sys)


    def get_latest_save_model_path(self):
        try:
            
            latest_dir=self.get_latest_save_model_path()
            return os.path.join(latest_dir, self.model_dir_name, config_entity.MODEL_FILE_NAME)

        except Exception as e:
            raise RetailException(e, sys)
        

    def get_latest_save_transform_data_path(self):
        try:
            
            latest_dir=self.get_latest_dir_path()
            return os.path.join(latest_dir, self.transformer_dir_name, config_entity.TRANSFORMATION_BASE_FILE_NAME)

        except Exception as e:
            raise RetailException(e, sys)
