from RentalBike.logger import logging
from RentalBike.exception import CustomException
import os, sys
from RentalBike.components.data_ingestion import DataIngestion
from RentalBike.entity.config_entity import DataIngestionConfig
from RentalBike.entity.artifact_entity import DataIngestionArtifact
from RentalBike.config.configuration import Configuration


class Training_Pipeline:
    def __init__(self, config: Configuration=Configuration()) -> None:
        try:
            logging.info(f"\n{'*'*20} Initiating Training Pipeline {'*'*20}\n\n")
            self.config = config
        except Exception as e:
            raise CustomException(e, sys) from e
    
    def start_data_ingestion(self, data_ingestion_config:DataIngestionConfig) -> DataIngestionArtifact:
        try:
            data_ingestion = DataIngestion(data_ingestion_config = data_ingestion_config)
            return data_ingestion.initiate_data_ingestion()
        except Exception as e:
            raise CustomException(e, sys)
        
    def run_training_pipeline(self):
        try:
            data_ingestion_config=self.config.get_data_ingestion_config()

            data_ingestion_artifact = self.start_data_ingestion(data_ingestion_config)
            
        except Exception as e:
            raise CustomException(e, sys) from e
        
    def __del__(self):
        logging.info(f"\n{'*'*20} Training Pipeline Complete {'*'*20}\n\n")