from Retail_transcation.exception import RetailException
from Retail_transcation.logger import logging
from Retail_transcation.components.data_ingestion import DataIngestion
from Retail_transcation.components.data_validation import DataValidation
from Retail_transcation.components.data_transformation import DataTransformation
from Retail_transcation.components.model_evaluation import ModelEvaluation
from Retail_transcation.components.model_trainer import ModelTrainer
from Retail_transcation import config, utils
from Retail_transcation.entity import config_entity, artifacts_entity
from Retail_transcation.predictor import ModelResolver



def StartTrainingPipeline():
    try:

        training_pipeline_config = config_entity.TrainingPipelineConfig()

        # data ingestion
        data_ingestion_config = config_entity.DataIngestionConfig(training_pipeline_config=training_pipeline_config)
        print(data_ingestion_config.convert())

        data_ingestion = DataIngestion(data_ingestion_config=data_ingestion_config)
        data_ingestion_artifact=data_ingestion.start_data_ingestion()


        # data validation:

        data_validation_config = config_entity.DataValidationConfig(training_pipeline_config=training_pipeline_config)
        data_validation= DataValidation(data_validation_config=data_validation_config,
                                        data_ingestion_artifacts=data_ingestion_artifact)
        data_validation_artifact=data_validation.initiate_data_validation()



        # data transformation:

        data_transformation_config=config_entity.DataTransformationConfig(training_pipeline_config=training_pipeline_config)
        data_transformation = DataTransformation(data_transformation_cofig=data_transformation_config,
                                                    data_validation_artifacts=data_validation_artifact)
        
        data_transformation_artifact=data_transformation.initiate_data_transformation()


        # model trainer

        model_trainer_config=config_entity.ModeTrainerConfig(training_pipeline_config=training_pipeline_config)
        model_trainer = ModelTrainer(model_trainer_config=model_trainer_config,
                                    data_transformation_artifacts=data_transformation_artifact)
        
        model_trainer_artifact=model_trainer.initiate_model_trainer()


        # model Evaluation
        model_evaluation_config = config_entity.ModeEvaluationConfig(training_pipeline_config=training_pipeline_config)
        model_evaluation = ModelEvaluation(model_evaluation_config=model_evaluation_config,
                                           data_ingestion_artifacts=data_ingestion_artifact,
                                           data_validation_artifacts=data_validation_artifact,
                                           data_transformation_artifacts=data_transformation_artifact,
                                           model_trainer_artifacts=model_trainer_artifact)
        
        model_evaluation_artifact = model_evaluation.initiate_model_evaluation()


    except Exception as e:
        raise RetailException(e,sys)