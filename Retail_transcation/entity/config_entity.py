import os, sys
from Retail_transcation.exception import RetailException
from Retail_transcation.logger import logging



FEATURE_FILE_NAME = "/home/vinod/projects/retail_transaction_pred/Online Retail.csv"
TRAIN_FILE_NAME = "train_df.csv"
TEST_FILE_NAME = "test_df.csv"
VALID_TRAIN_FILE="valid_train.csv"
VALID_TEST_FILE="valid_test.csv"
TRANS_TRAIN_FILE_NAME="trans_train.npy"
TRANS_TEST_FILE_NAME="trans_test.npy"
TRANSFORMATION_FILE_NAME='transformer.pkl'

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

            data_split_dir = os.path.join(self.data_ingestion_dir, "data_split")
            os.makedirs(data_split_dir, exist_ok=True)

            self.train_path = os.path.join(data_split_dir, TRAIN_FILE_NAME)
            self.test_path = os.path.join(data_split_dir, TEST_FILE_NAME)
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

            valid_data_split_dir = os.path.join(self.data_validation_dir, "data_split")
            os.makedirs(valid_data_split_dir, exist_ok=True)

            self.valid_train_path = os.path.join(valid_data_split_dir, VALID_TRAIN_FILE)
            self.valid_test_path = os.path.join(valid_data_split_dir, VALID_TEST_FILE)

            self.report_file_path = os.path.join(self.data_validation_dir, "report.yaml")

            
        except Exception as e:
            raise RetailException(e,sys)

class DataTransformationConfig:

    def __init__(self, training_pipeline_config: TrainingPipelineConfig):
        try:
            
            self.data_transformation_dir = os.path.join(training_pipeline_config.artifact_dir,"data_transformation")
            os.makedirs(self.data_transformation_dir, exist_ok=True)  # Create the data_validation directory if it doesn't exist

            transform_data_split_dir = os.path.join(self.data_transformation_dir, "data_split")
            os.makedirs(transform_data_split_dir, exist_ok=True)

            self.transform_train_path = os.path.join(transform_data_split_dir, TRANS_TRAIN_FILE_NAME)
            self.transform_test_path = os.path.join(transform_data_split_dir, TRANS_TEST_FILE_NAME)
            self.pre_process_object_path = os.path.join(self.data_transformation_dir, "Pre_process_model", TRANSFORMATION_FILE_NAME)
            self.single_pred_data_path = os.path.join(self.data_transformation_dir, "Pre_process_model", "single_pred_data.pkl")

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
            #self.data_path = os.path.join(model_dir, "data.pkl")

            self.expected_r2_score = 0.5
            self.overfitting_value = 0.3

        except Exception as e:
            raise RetailException(e,sys)
        


class ModeEvaluationConfig:

    def __init__(self, training_pipeline_config:TrainingPipelineConfig):
        try:

            self.change_overfitting_value = 0.01

        except Exception as e:
            raise RetailException(e,sys)
        

class ModelPusherConfig:
    def __init__(self, training_pipeline_config:TrainingPipelineConfig):
        self.model_pusher_dir = os.path.join(training_pipeline_config.artifact_dir, "Model_pusher")
        os.makedirs(self.model_pusher_dir, exist_ok=True)

        self.saved_model_dir = os.path.join("saved_models")
        #os.makedirs(self.saved_model_dir, exist_ok=True)

        self.pusher_model_dir = os.path.join(self.model_pusher_dir,"saved_models")
        #os.makedirs(self.pusher_model_dir, exist_ok=True)

        self.pusher_model_path = os.path.join(self.pusher_model_dir, MODEL_FILE_NAME)
        self.pusher_transform_path = os.path.join(self.pusher_model_dir, TRANSFORMATION_FILE_NAME)
