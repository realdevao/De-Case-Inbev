# Brewery Data Pipeline

### Project Overview
This project demonstrates a data pipeline for extracting, transforming, and loading brewery data from the Open Brewery DB API. The data pipeline is built using Apache Airflow and follows the Medallion Architecture with three layers:

* Bronze Layer: Raw data fetched from the API is stored in JSON format.
* Silver Layer: The raw data is transformed, filtered, and partitioned by brewery location (state) and stored in Parquet format.
* Gold Layer: Aggregated data is stored, providing insights into the number of breweries by type and location.
  
The pipeline is containerized using Docker, making it easy to set up and run locally. It also includes unit tests to ensure the functionality of each step in the pipeline.


### Architecture
The data pipeline follows a Medallion Architecture with three layers:

1. Bronze Layer: This layer ingests raw data from the Open Brewery API and saves it in JSON format. No transformation is performed at this stage.
2. Silver Layer: In this layer, the data is cleaned, filtered (removing records with null values in critical fields), and stored in a columnar format (Parquet). The data is partitioned by brewery location for better query performance.
3. Gold Layer: The final layer contains aggregated data that can be used for analytics. This includes the count of breweries by type and location (state).


### Requirements
To run this project, ensure you have the following installed:

* Docker
* Docker Compose


### Project Structure

~~~bash
├── dags/                
│   ├── bronze_layer_brewery_data.py     # DAG for Bronze Layer ingestion
│   ├── silver_layer_transform_data.py   # DAG for Silver Layer transformation
│   └── gold_layer_aggregate_data.py     # DAG for Gold Layer aggregation
├── tests/                               
│   ├── test_bronze_layer.py             # Unit tests for Bronze Layer
│   ├── test_silver_layer.py             # Unit tests for Silver Layer
│   └── test_gold_layer.py               # Unit tests for Gold Layer
├── docker-compose.yml                   # Docker Compose orchestration file
├── Dockerfile                           # Docker configuration file for Airflow
├── README.md                            # Project documentation
~~~


### Setup Instructions

#### 1. Clone the repository
~~~bash
git clone https://github.com/username/brewery-data-pipeline.git
cd brewery-data-pipeline
~~~

#### 2. Start the Airflow environment with Docker Compose
Run the following command to spin up Airflow and other required services:
~~~bash
docker-compose up -d
~~~~
Airflow will be available at <http://localhost:8080/>. Use the default credentials:
* Username: airflow
* Password: airflow

#### 3. Access Airflow
Go to <http://localhost:8080/> and log in with the credentials provided above. You should see the DAGs listed:
* bronze_layer_brewery_data
* silver_layer_transform_data
* gold_layer_aggregate_data
Activate and trigger each DAG to run the pipeline.

#### 4. Running Unit Tests
This project includes unit tests to validate each step of the pipeline. To run the tests, use the following command:
~~~bash
pytest tests/
~~~
The tests cover the API data fetching, data transformation, and aggregation processes.


### Data Lake Structure
1. Bronze Layer: Raw data from the API is saved in JSON format in the following directory:
* /opt/airflow/bronze_layer/breweries_raw.json

2. Silver Layer: Data is transformed and partitioned by state and saved in Parquet format in the following directory:
* /opt/airflow/silver_layer/breweries_partitioned/

3. Gold Layer: Aggregated data showing the number of breweries by type and location is saved in Parquet format:
* /opt/airflow/gold_layer/breweries_aggregated.parquet


### Medallion Layer Details
#### Bronze Layer
* Source: Open Brewery DB API
* Format: JSON
* Data Transformation: None (raw data ingestion)

#### Silver Layer
* Transformation:
* Removing null values from the state column.
* Converting the data to Parquet format.
* Partitioning the data by state for efficient querying.

#### Gold Layer
* Aggregation:
* Grouping the data by state and brewery_type.
* Storing the result in Parquet format with the count of breweries per type and location.


### Cloud Integration (Optional)
This pipeline is designed to be expandable to cloud environments such as AWS or Google Cloud. Here are some options for cloud integration:

#### AWS S3
You can modify the storage paths to store the data in Amazon S3:
~~~python
df.to_parquet('s3://my-bucket/silver_layer/breweries_partitioned', partition_cols=['state'])
~~~
Make sure to configure your AWS credentials within Airflow using environment variables or AWS IAM roles.

#### Google Cloud Storage (GCS)
Similarly, you can store the data in GCS:

~~~python
df.to_parquet('gs://my-bucket/silver_layer/breweries_partitioned', partition_cols=['state'])
~~~


### Future Improvements
Here are some ideas for future improvements to this project:

* Advanced Data Validation: Add further validation steps in the Silver Layer, such as field format checks and deduplication.
* Cloud Deployment: Implement a cloud-based data lake using services like AWS S3 or Google Cloud Storage.
* Business Intelligence Integration: Connect the Gold Layer to a BI tool like Power BI or Tableau for visualizing the data insights.
* Additional APIs: Extend the pipeline to fetch and process data from other public APIs.


### Conclusion
This project implements a complete ETL pipeline using Apache Airflow to consume, transform, and aggregate data following the Medallion Architecture. It includes layers for raw data, curated data, and aggregated data, all containerized in Docker for easy setup.

Feel free to fork this project and expand upon it!
