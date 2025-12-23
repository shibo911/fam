import pandas as pd
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def load_data(file_path: Path) -> pd.DataFrame:
    """
    Loads CSV, enforces types, and removes duplicates.
    """
    if not file_path.exists():
        logger.error(f"File not found: {file_path}")
        raise FileNotFoundError(f"The file {file_path} was not found.")
    
    try:
        df = pd.read_csv(file_path, parse_dates=['date'])
        
        # 1. Enforce Numeric Types (Coerce errors to NaN)
        numeric_cols = ['open', 'high', 'low', 'close', 'volume']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # 2. Deduplicate
        initial_rows = len(df)
        df = df.drop_duplicates(subset=['ticker', 'date'])
        if len(df) < initial_rows:
            logger.warning(f"Dropped {initial_rows - len(df)} duplicate rows.")

        # 3. Sort
        df = df.sort_values(by=['ticker', 'date']).reset_index(drop=True)
        
        logger.info(f"Data loaded successfully. Shape: {df.shape}")
        return df
        
    except Exception as e:
        logger.error(f"Failed to load data: {e}")
        raise e