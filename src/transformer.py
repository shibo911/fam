import pandas as pd
import logging

logger = logging.getLogger(__name__)

def resample_to_monthly(df: pd.DataFrame) -> pd.DataFrame:
    """
    Resamples daily data to monthly frequency per ticker.
    Aggregates using OHLC logic (Open=First, Close=Last).
    """
    # Explicitly check if required columns exist
    required_cols = ['open', 'high', 'low', 'close']
    if not all(col in df.columns for col in required_cols):
        raise ValueError(f"Input dataframe missing required columns: {required_cols}")

    agg_logic = {
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last'
    }
    
    # Handle Volume if present
    if 'volume' in df.columns:
        agg_logic['volume'] = 'sum'

    # Note on Adjusted Close:
    # Requirements specify indicators based on monthly closing prices.
    # In production, Adjusted Close is often preferred to account for splits and dividends.
    # This implementation uses the raw 'close' to match the specification's snapshot requirement.

    logger.info("Resampling data to Month End frequency...")
    
    # Use 'M' instead of 'ME' for broader compatibility
    monthly_df = df.set_index('date').groupby('ticker').resample('M').agg(agg_logic)
    
    return monthly_df.reset_index()

def calculate_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates SMA (10, 20) and EMA (10, 20) using vectorized operations.
    """
    logger.info("Calculating SMA and EMA indicators...")
    
    # SMA Calculation
    df['sma_10'] = df.groupby('ticker')['close'].transform(lambda x: x.rolling(window=10).mean())
    df['sma_20'] = df.groupby('ticker')['close'].transform(lambda x: x.rolling(window=20).mean())

    # EMA Calculation
    # adjust=False calculates the recursive formula required by technical analysis
    df['ema_10'] = df.groupby('ticker')['close'].transform(lambda x: x.ewm(span=10, adjust=False).mean())
    df['ema_20'] = df.groupby('ticker')['close'].transform(lambda x: x.ewm(span=20, adjust=False).mean())

    return df

def run_transformations(df: pd.DataFrame) -> pd.DataFrame:
    """Orchestrator for all data transformations."""
    monthly_df = resample_to_monthly(df)
    final_df = calculate_technical_indicators(monthly_df)
    
    # FINAL POLISH: Deterministic sorting
    # Ensures output is strictly ordered by Ticker then Date
    final_df = final_df.sort_values(by=['ticker', 'date'])
    
    return final_df