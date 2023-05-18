import pandas as pd
import numpy as np
import os, sys
from Retail_transcation.exception import RetailException
from Retail_transcation.logger import logging
from Retail_transcation.entity import config_entity, artifacts_entity


class DataIngestion:
    
    def __init__(self, data_ingestion_config: config_entity.DataIngestionConfig):
        try:
            self.data_ingestion_config = data_ingestion_config
        except Exception as e:
            raise RetailException(e, sys)
        



