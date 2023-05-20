from Retail_transcation.exception import RetailException
from Retail_transcation.logger import logging
from Retail_transcation.components.data_ingestion import DataIngestion
from Retail_transcation import utils
from Retail_transcation.components.data_ingestion import DataIngestion
from Retail_transcation.entity import config_entity, artifacts_entity
import os, sys





if __name__=="__main__":
    try:
        
        #utils.get_as_df(database_name="online_retail", collection_name="online_retail")

        training_pipeline_config = config_entity.TrainingPipelineConfig()
        data_ingestion_config = config_entity.DataIngestionConfig(training_pipeline_config=training_pipeline_config)
        print(data_ingestion_config.convert())

        data_ingestion = DataIngestion(data_ingestion_config=data_ingestion_config)
        data_ingestion_artifact=data_ingestion.start_data_ingestion()

    except Exception as e:
        print(e)