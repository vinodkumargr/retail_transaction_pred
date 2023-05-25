from dataclasses import dataclass

@dataclass
class DataIngestionArtifact:
    feature_store_path:str

@dataclass
class DataValidationArtifact:
    report_file_path:str
    valid_feature_store_path:str


@dataclass
class DataTransformationArtifact:
    transform_feature_store_path:str
    transform_train_path:str
    transform_test_path:str


@dataclass
class ModelTrainerArtifact:
    model_path:str
    r2_train_score:float
    r2_test_score:float


@dataclass
class ModelEvaluationArtifact:
    model_eccepted:bool
    improved_accuracy:float