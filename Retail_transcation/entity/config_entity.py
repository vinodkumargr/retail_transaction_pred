import os, sys
from Retail_transcation.exception import RetailException
from Retail_transcation.logger import logging



FILE_NAME = "final_data.csv"
TRAIN_FILE_NAME = "train.csv"
TEST_FILE_NAME = "test.csv"


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







