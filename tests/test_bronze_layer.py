import os
import pytest
import requests
from requests.exceptions import RequestException, Timeout, HTTPError
import logging

# Environment settings
API_URL = os.getenv('BREWERY_API_URL', 'https://api.openbrewerydb.org/breweries')

# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to fetch data from the API with retries and timeout
def fetch_brewery_data(url, retries=3, timeout=5):
    for attempt in range(retries):
        try:
            logging.info(f"Attempt {attempt + 1} to fetch data from {url}")
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()  # Levanta exceção para status 4xx/5xx
            return response.json()
        except (RequestException, Timeout, HTTPError) as e:
            logging.error(f"Error on attempt {attempt + 1}: {e}")
            if attempt == retries - 1:
                raise

# Simulating API response
@pytest.fixture
def mock_api_response(requests_mock):
    mock_response = [
        {"id": 9094, "name": "MadTree Brewing", "brewery_type": "regional", "state": "Ohio"},
        {"id": 9095, "name": "BrewDog", "brewery_type": "micro", "state": "Ohio"}
    ]
    requests_mock.get(API_URL, json=mock_response)
    return mock_response

# Test to verify data request from the API
def test_fetch_brewery_data(mock_api_response):
    data = fetch_brewery_data(API_URL)

    assert isinstance(data, list)
    assert len(data) > 0
    assert data == mock_api_response

# Test to verify request failure
def test_fetch_brewery_data_failure(requests_mock):
    requests_mock.get(API_URL, exc=RequestException)

    with pytest.raises(RequestException):
        fetch_brewery_data(API_URL)

# Test to verify timeout
def test_fetch_brewery_data_timeout(requests_mock):
    requests_mock.get(API_URL, exc=Timeout)

    with pytest.raises(Timeout):
        fetch_brewery_data(API_URL)
