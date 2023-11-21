
from RentalBike.logger import logging
from RentalBike.exception import CustomException
from RentalBike.entity.config_entity import ModelPusherConfig
from RentalBike.entity.artifact_entity import ModelTrainerArtifact, ModelPusherArtifact
import os
import shutil

class ModelPusher:
    def __init__(self, model_pusher_config: ModelPusherConfig, model_trainer_artifact: ModelTrainerArtifact):
        try:
            logging.info("Model Pusher log started.")
            self.model_pusher_config = model_pusher_config
            self.model_trainer_config = model_trainer_artifact
        except Exception as e:
            logging.error(f"Error in ModelPusher initialization: {e}")
            raise CustomException(e)

    def export_model(self) -> ModelPusherArtifact:
        try:
            evaluated_model_file_path = self.model_trainer_config.trained_model_object_file_path
            export_dir = self.model_pusher_config.export_dir_path
            model_file_name = os.path.basename(evaluated_model_file_path)
            export_model_file_path = os.path.join(export_dir, model_file_name)
            logging.info(f'Exporting model file: [{export_model_file_path}]')
            os.makedirs(export_dir, exist_ok=True)

            shutil.copy(src=evaluated_model_file_path, dst=export_model_file_path)

            logging.info(f"Trained Model: {evaluated_model_file_path} is copied in export dir: [{export_model_file_path}]")

            model_pusher_artifact = ModelPusherArtifact(is_model_pusher=True, export_model_file_path=export_model_file_path)

            logging.info(f"Model Pusher Artifact: [{model_pusher_artifact}]")
            return model_pusher_artifact
        except Exception as e:
            logging.error(f"Error in exporting model: {e}")
            raise CustomException(e)

    def initiate_model_pusher(self) -> ModelPusherArtifact:
        try:
            return self.export_model()
        except Exception as e:
            logging.error(f"Error in initiating model pusher: {e}")
            raise CustomException(e)

    def __del__(self):
        logging.info("Model Pusher log completed.")




























"""class ModelPusher:
    def __init__(self, model_pusher_config: ModelPusherConfig,
                 model_trainer_artifact: ModelTrainerArtifact):
        
        try:
            logging.info(f"{'>>' * 20 } Model Pusher log started.{'<<' * 20}")
            self.model_pusher_config = model_pusher_config
            self.model_trainer_config = model_trainer_artifact
        except Exception as e:
            raise ApplicationException(e,sys)
        
    def export_model(self) -> ModelPusherConfig:
        try:
            evaluated_model_file_path = self.model_trainer_config.trained_model_object_file_path
            export_dir = self.model_pusher_config.export_dir_path
            model_file_name = os.path.basename(evaluated_model_file_path)
            export_model_file_path = os.path.join(export_dir, model_file_name)
            logging.info(f'Exporting model file : [{export_model_file_path}]')
            os.makedirs(export_dir,exist_ok= True)

            shutil.copy(src=evaluated_model_file_path, dst=export_model_file_path)

            logging.info(f"Trained Model : {evaluated_model_file_path} is copied in export dir : [{export_model_file_path}]")

            model_pusher_artifact = ModelPusherArtifact(is_model_pusher=True, export_model_file_path=export_model_file_path)

            logging.info(f"Model Pusher Artifact: [{model_pusher_artifact}")
            return model_pusher_artifact
        except Exception as e:
            raise ApplicationException(e,sys)
        

    def initiate_model_pusher(self) -> ModelPusherArtifact:
        try:
            return self.export_model
        except Exception as e:
            raise ApplicationException(e,sys)
        
    def __del__(self):
        logging.info(f"{'>>'*20}Model Pusher log completed.{'<<'*20}")"""