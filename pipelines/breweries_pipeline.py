from src.ingestion.api_client import fetch_breweries, save_bronze
from src.processing.bronze_to_silver import transform_bronze_to_silver
from src.processing.silver_to_gold import transform_silver_to_gold
from src.utils.logger import get_logger

logger = get_logger(__name__)


def run_pipeline():

    logger.info("Starting ingestion")

    data = fetch_breweries()

    save_bronze(data)

    logger.info("Starting Bronze → Silver")

    transform_bronze_to_silver()

    logger.info("Starting Silver → Gold")

    transform_silver_to_gold()

    logger.info("Pipeline finished successfully")


if __name__ == "__main__":
    run_pipeline()
