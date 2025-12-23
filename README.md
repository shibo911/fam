# Financial Data Engineering Assignment

A modular ETL pipeline designed to process daily stock data into monthly summaries with technical indicators (SMA/EMA).

## Project Overview
- **Goal:** Resample 2-year daily stock data to monthly OHLC snapshots and calculate technical indicators.
- **Input:** CSV containing daily data for 10 tickers.
- **Output:** 10 partitioned CSV files (one per ticker), each containing exactly 24 monthly rows.

## Prerequisites
- Python 3.8+
- Pandas 2.2.0 (Required for `ME` offset alias)

## Quick Start
1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

1.  Run the pipeline:

    

    ```
    python main.py
    ```

    Optional CLI usage:

    

    ```
    python main.py --input data/raw/my_data.csv --output data/results/
    ```

2.  Run verification tests:

    

    ```
    python verify.py
    ```

Design Decisions and Logic
--------------------------

### 1\. Financial Logic (Close vs. Adj Close)

The input dataset provides both Close and Adj Close.

-   **Decision:** I have used the raw **Close** price for all aggregations and indicator calculations.

    -   **Reasoning:** The problem statement explicitly requested snapshots of "Close" prices. While Adj Close is standard in trading strategies to account for splits/dividends, using it here would deviate from the specific requirements of the assignment to capture the price "at the last trading day."

### 2\. Handling the 24-Row Constraint

-   **Edge Case:** SMA-20 requires 20 periods of data. For the first 19 months, the result is mathematically undefined (NaN).

    -   **Resolution:** To strictly satisfy the requirement that "Each file must contain exactly 24 rows," I retain these NaN rows rather than dropping them.

### 3\. Architecture

-   **Modular Design:** Logic is separated into loader, transformer, and writer for testability.

    -   **Vectorization:** Used Pandas groupby().transform() and ewm() to avoid inefficient loops.

    -   **Path Handling:** Used pathlib for cross-platform compatibility.

### 4. Math Implementation Details
*   **EMA Initialization:** 
    *   Standard formula: Often seeds the first EMA value using the Simple Moving Average (SMA) of the first N periods.
    *   **Implementation:** I utilized Pandas `ewm(span=N, adjust=False)`. This function calculates EMA recursively starting from the *first available data point* rather than waiting for N periods to calculate an initial SMA seed. This is a standard approach in Python financial analysis and ensures coverage for the earlier data points, aligning with the assignment's vectorization requirement.
*   **Aggregation Logic:** 
    *   OHLC data is aggregated strictly based on snapshots: `Open` is the price on the first day, `Close` is the price on the last day. `High`/`Low` are the absolute max/min over the month.

### 5\. Code Hygiene (Sort Order)

In src/transformer.py, add a sort at the very end to ensure the CSV is ordered chronologically.



```
# ... inside run_transformations ...
def run_transformations(df: pd.DataFrame) -> pd.DataFrame:
    # ... existing code ...
    monthly_df = resample_to_monthly(df)
    final_df = calculate_technical_indicators(monthly_df)

    # Ensure deterministic output order
    final_df = final_df.sort_values(by=['ticker', 'date'])

    return final_df
```



### 6. Date Labeling Semantics
*   **Logic:** The output files use the **calendar month-end date** (e.g., `2018-01-31`) as the label for the row. 
*   **Reasoning:** While the actual "Last Trading Day" might be `2018-01-30`, standard financial time-series resampling (using Pandas `ME` offset) aligns to the period end. The *values* (Close price) strictly reflect the actual last trading day's data, but the *index label* is normalized to the month-end for consistency across tickers.

### 7. Pandas Compatibility
*   **Requirement:** This project uses `resample('ME')` which is the standard alias in **Pandas 2.2+**. 
*   **Backward Compatibility:** If running on older Pandas versions, this alias may cause a `ValueError`. Please ensure you install the dependencies in `requirements.txt` (`pandas==2.2.0`).

Testing
-------

A verify.py script is included to validate:

-   Output file count (10 files).

    -   Row count invariant (24 rows per file).

    -   Column schema correctness.

## Data Setup
1. Download the dataset from the [provided GitHub link](https://github.com/sandeep-tt/tt-intern-dataset).
2. Save the CSV file as `output_file.csv`.
3. Place it inside the `data/raw/` directory.
   ```text
   data/
   └── raw/
       └── output_file.csv
```

About the Author
----------------
- Shivang Agrahari — prefinal year student at IIIT Lucknow