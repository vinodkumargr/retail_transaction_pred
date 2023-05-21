from dataclasses import dataclass

@dataclass
class DataIngestionArtifact:
    feature_store_path:str
    train_file_path:str
    test_file_path:str

@dataclass
class DataValidationArtifact:
    report_file_path:str

@dataclass
class DataTransformationConfig:
    transform_train_path:str
    transform_test_path:str

