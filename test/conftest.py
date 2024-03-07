import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope = "session")
def spark():
    '''This function is responsible for creating spark session for testing module.'''
    return(SparkSession.builder.appName('testing').getOrCreate())