import os
import pandas as pd
import pytest
import logging

# Environment settings
PARTITION_COLS = os.getenv('PARTITION_COLS', 'state').split(',')
OUTPUT_DIR = os.getenv('SILVER_OUTPUT_DIR', '/tmp/silver_layer')

# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to transform data (removing nulls)
def transform_data(df):
    logging.info("Starting data transformation.")
    df_clean = df.dropna(subset=['state'])
    logging.info(f"Rows with nulls in the 'state' column removed: {df.shape[0] - df_clean.shape[0]}")
    return df_clean

# Function to save partitioned data
def save_partitioned_data(df, output_dir, partition_cols):
    logging.info(f"Saving partitioned data to {output_dir}")
    df.to_parquet(output_dir, partition_cols=partition_cols)
    logging.info("Partitioned data saved successfully.")

# Test data fixture
@pytest.fixture
def sample_brewery_data():
    data = [
        {"id": 1, "name": "MadTree Brewing", "brewery_type": "regional", "state": "Ohio"},
        {"id": 2, "name": "BrewDog", "brewery_type": "micro", "state": "Ohio"},
        {"id": 3, "name": "Great Lakes Brewing", "brewery_type": "brewpub", "state": None},
    ]
    return pd.DataFrame(data)

# Test for transformation and removal of nulls
def test_transform_data(sample_brewery_data):
    df_clean = transform_data(sample_brewery_data)
    
    # Validation of the transformation result
    assert df_clean.shape[0] == 2
    assert df_clean['state'].notna().all()

# Partitioning test
def test_partitioning(sample_brewery_data, tmpdir):
    output_dir = tmpdir.mkdir("silver_layer")
    
    df_clean = transform_data(sample_brewery_data)
    save_partitioned_data(df_clean, str(output_dir), PARTITION_COLS)

    # Checking if the partition was created correctly
    partitioned_files = list(output_dir.listdir())
    assert len(partitioned_files) > 0

    # Verifying if the partitions contain the correct directories
    partition_dirs = [dir.basename for dir in partitioned_files]
    assert all(col in partition_dirs[0] for col in PARTITION_COLS)

# Schema test
def test_schema(sample_brewery_data):
    expected_columns = ['id', 'name', 'brewery_type', 'state']
    assert list(sample_brewery_data.columns) == expected_columns
