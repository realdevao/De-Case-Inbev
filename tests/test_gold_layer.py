import os
import pandas as pd
import pytest
import logging

# Environment settings
AGGREGATION_COLS = os.getenv('AGGREGATION_COLS', 'state').split(',')

# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to perform aggregations
def aggregate_data(df, aggregation_cols):
    logging.info("Starting data aggregation.")
    
    # Remove nulls
    df_clean = df.dropna(subset=aggregation_cols)
    
    # Perform aggregation
    aggregated_df = df_clean.groupby(aggregation_cols).size().reset_index(name='brewery_count')
    
    logging.info("Aggregation completed successfully.")
    return aggregated_df

# Test data fixture
@pytest.fixture
def sample_brewery_data():
    data = {
        'state': ['Ohio', 'Ohio', 'California', None],
        'brewery_type': ['regional', 'micro', 'micro', 'regional']
    }
    return pd.DataFrame(data)

# Data aggregation test
def test_aggregate_data(sample_brewery_data):
    aggregated_df = aggregate_data(sample_brewery_data, AGGREGATION_COLS)
    
    # Aggregation validations
    assert 'brewery_count' in aggregated_df.columns
    assert aggregated_df.shape[0] == 2 
    assert aggregated_df.loc[aggregated_df['state'] == 'Ohio', 'brewery_count'].values[0] == 2

# Test with null data
def test_aggregate_data_with_nulls(sample_brewery_data):
    df_clean = sample_brewery_data.dropna(subset=AGGREGATION_COLS)
    assert df_clean.shape[0] == 3
