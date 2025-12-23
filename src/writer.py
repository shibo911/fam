import pandas as pd
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def save_partitioned_data(df: pd.DataFrame, output_dir: Path) -> None:
    """
    Partitions the dataframe by Ticker and saves individual CSV files.
    Ensures exactly 24 rows per file.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    
    grouped = df.groupby('ticker')
    count = 0
    
    for ticker, data in grouped:
        filename = f"result_{ticker}.csv"
        file_path = output_dir / filename
        
        # Drop ticker column for the individual file output
        output_data = data.drop(columns=['ticker'])
        
        # STRICT VALIDATION: The 24-row constraint
        # We raise an error instead of a warning to ensure data integrity
        if len(output_data) != 24:
             error_msg = f"Ticker {ticker} has {len(output_data)} rows. Requirement is exactly 24."
             logger.error(error_msg)
             raise ValueError(error_msg)
        
        # Write to CSV
        output_data.to_csv(file_path, index=False)
        count += 1
        
    logger.info(f"Successfully created {count} files in {output_dir}")