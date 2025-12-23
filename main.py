import argparse
import logging
from pathlib import Path
from src.loader import load_data
from src.transformer import run_transformations
from src.writer import save_partitioned_data

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Defaults
DEFAULT_INPUT = Path('data/raw/output_file.csv')
DEFAULT_OUTPUT = Path('data/output')

def main():
    parser = argparse.ArgumentParser(description="Financial Data ETL Pipeline")
    parser.add_argument('--input', type=Path, default=DEFAULT_INPUT, help="Path to input CSV")
    parser.add_argument('--output', type=Path, default=DEFAULT_OUTPUT, help="Directory for output CSVs")
    args = parser.parse_args()

    # Begin pipeline execution
    logger.info("Starting Data Engineering Pipeline...")

    try:
        # 1. Extract
        raw_df = load_data(args.input)

        # 2. Transform
        processed_df = run_transformations(raw_df)

        # 3. Load
        save_partitioned_data(processed_df, args.output)
        
        # Indicate successful completion of the pipeline
        logger.info("Pipeline completed successfully.")
        
    except Exception as e:
        logger.critical(f"Pipeline failed: {e}")
        exit(1)

if __name__ == "__main__":
    main()