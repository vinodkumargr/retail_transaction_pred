import os, sys
from Retail_transcation.exception import RetailException
from Retail_transcation.logger import logging



FILE_NAME = "/home/vinod/projects/retail_transaction_pred/Online Retail.csv"
TRAIN_FILE_NAME = "train.csv"
TEST_FILE_NAME = "test.csv"
TRANSFORMATION_BASE_FILE_NAME = "transformed_base_df.csv"
TRANSFORMATION_TRAIN_FILE_NAME = "transformed_train_df.csv"
TRANSFORMATION_TEST_FILE_NAME = "transformed_test_df.csv"
MODEL_FILE_NAME = "model.pkl"


class TrainingPipelineConfig:
    
    def __init__(self):
        try:
            self.artifact_dir = os.path.join(os.getcwd(),"artifacts")
        except Exception  as e:
            raise RetailException(e,sys) 
        
    
class DataIngestionConfig:

    def __init__(self,training_pipeline_config: TrainingPipelineConfig):
        try:

            self.database_name="online_retail"
            self.collection_name="online_retail"
            self.data_ingestion_dir = os.path.join(training_pipeline_config.artifact_dir, "data_ingestion")
            self.feature_store_path = os.path.join(self.data_ingestion_dir, "feature_store", FILE_NAME)
            self.train_file_path = os.path.join(self.data_ingestion_dir,"data_split", TRAIN_FILE_NAME)
            self.test_file_path = os.path.join(self.data_ingestion_dir, "data_split", TEST_FILE_NAME)

        except Exception as e:
            raise RetailException(e,sys)
        

    # Convert data into dict
    def convert(self):
        try:
            print_data = self.__dict__
            logging.info(f"printing dict data : {print_data}")
            return print_data
        except Exception  as e:
            raise RetailException(e,sys)


class DataValidationConfig:

    def __init__(self, training_pipeline_config: TrainingPipelineConfig):
        try:

            self.data_validation_dir = os.path.join(training_pipeline_config.artifact_dir, "data_validation")
            os.makedirs(self.data_validation_dir, exist_ok=True)  # Create the data_validation directory if it doesn't exist

            feature_store_dir = os.path.join(self.data_validation_dir, "feature_store")
            os.makedirs(feature_store_dir, exist_ok=True)  # Create the feature_store directory if it doesn't exist
            data_split_dir = os.path.join(self.data_validation_dir, "data_split")
            os.makedirs(data_split_dir, exist_ok=True)  # Create the data_split directory if it doesn't exist

            self.valid_feature_store_path = os.path.join(feature_store_dir, "validation_base.csv")
            self.valid_train_file_path = os.path.join(data_split_dir, "validation_train.csv")
            self.valid_test_file_path = os.path.join(data_split_dir, "validatin_test.csv")
            self.report_file_path = os.path.join(self.data_validation_dir, "report.yaml")

            self.base_file_path = os.path.join(FILE_NAME)



            
        except Exception as e:
            raise RetailException(e,sys)

class DataTransformationConfig:

    def __init__(self, training_pipeline_config: TrainingPipelineConfig):
        try:
            
            self.data_transformation_dir = os.path.join(training_pipeline_config.artifact_dir,"data_transformation")
            os.makedirs(self.data_transformation_dir, exist_ok=True)  # Create the data_validation directory if it doesn't exist

            feature_store_dir = os.path.join(self.data_transformation_dir, "feature_store")
            os.makedirs(feature_store_dir, exist_ok=True)  # Create the feature_store directory if it doesn't exist
            data_split_dir = os.path.join(self.data_transformation_dir, "data_split")
            os.makedirs(data_split_dir, exist_ok=True)  # Create the data_split directory if it doesn't exist

            self.transform_feature_store_path = os.path.join(feature_store_dir, TRANSFORMATION_BASE_FILE_NAME)
            self.transform_train_file_path = os.path.join(data_split_dir, TRANSFORMATION_TRAIN_FILE_NAME)
            self.transform_test_file_path = os.path.join(data_split_dir, TRANSFORMATION_TEST_FILE_NAME)

        except Exception as e:
            raise RetailException(e,sys)
        

class ModeTrainerConfig:

    def __init__(self, training_pipeline_config:TrainingPipelineConfig):
        try:

            self.model_trainer_dir = os.path.join(training_pipeline_config.artifact_dir,"model_trainer")
            os.makedirs(self.model_trainer_dir, exist_ok=True)

            model_dir = os.path.join(self.model_trainer_dir, "model")
            os.makedirs(model_dir, exist_ok=True)

            self.model_path = os.path.join(model_dir,MODEL_FILE_NAME)

            self.expected_r2_score = 0.75
            self.overfitting_value = 0.3

        except Exception as e:
            raise RetailException(e,sys)
        


class ModeEvaluationConfig:

    def __init__(self, training_pipeline_config:TrainingPipelineConfig):
        try:

            self.change_overfitting_value = 0.01

        except Exception as e:
            raise RetailException(e,sys)