from dataclasses import dataclass

@dataclass
class DataIngestionArtifact:
    train_data_path:str
    test_data_path:str

@dataclass
class DataValidationArtifact:
    report_file_path:str
    valid_train_path:str
    valid_test_path:str


@dataclass
class DataTransformationArtifact:
    transform_train_path:str
    transform_test_path:str
    pre_process_object_path:str


@dataclass
class ModelTrainerArtifact:
    model_path:str
    r2_train_score:float
    r2_test_score:float


@dataclass
class ModelEvaluationArtifact:
    model_eccepted:bool
    improved_accuracy:float


@dataclass
class ModelPusherArtifact:
    pusher_model_dir:str
    saved_model_dir:str