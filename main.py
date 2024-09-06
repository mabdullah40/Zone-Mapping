import logging
import os
from pipeline.data_ingestion import DataIngestionPipeline
from pipeline.regex_processing import RegexProcessingPipeline
from pipeline.api_processing import APIGeocodingPipeline
from pipeline.warehouse_mapping import WarehouseMappingPipeline
from pipeline.data_write import DataWritingPipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def run_pipeline():
    STAGE_NAME = "DATA INGESTION"
    try:
        logger.info(f">>> STAGE {STAGE_NAME} STARTED <<<")
        obj = DataIngestionPipeline()
        data_changed = obj.main()
        logger.info(f">>> STAGE {STAGE_NAME} COMPLETED <<<")

        if not data_changed:
            logger.info("No changes in data. Stopping pipeline execution.")
            return
    except Exception as e:
        logger.error(f">>> STAGE {STAGE_NAME} FAILED <<<")
        logger.exception(e)
        raise e

    # If data has changed, continue with the rest of the pipeline
    stages = [
        ("REGEX PROCESSING", RegexProcessingPipeline),
        ("API PROCESSING", APIGeocodingPipeline),
        ("WAREHOUSE MAPPING", WarehouseMappingPipeline),
        ("DATA WRITING", DataWritingPipeline),
    ]

    for stage_name, pipeline_class in stages:
        try:
            logger.info(f">>> STAGE {stage_name} STARTED <<<")
            obj = pipeline_class()
            obj.main()
            logger.info(f">>> STAGE {stage_name} COMPLETED <<<")
        except Exception as e:
            logger.error(f">>> STAGE {stage_name} FAILED <<<")
            logger.exception(e)
            raise e


if __name__ == "__main__":
    run_pipeline()
