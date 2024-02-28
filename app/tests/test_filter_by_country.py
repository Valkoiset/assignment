from chispa.dataframe_comparer import assert_df_equality
# from kommati_para.cleaning import filter_by_country
from pyspark.sql import SparkSession
import pytest
from ..package.cleaning import filter_by_country


@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder.appName('TestFilterByCountry').getOrCreate()


def test_filter_by_country(spark):
    data = [
        ('Anthony Kiedis', 'USA'),
        ('John Frusciante', 'USA'),
        ('Freddie Mercury', 'United Kingdom'),
        ('Eva Simons', 'Netherlands'),
        ('Alex Turner', 'United Kingdom')
    ]
    df = spark.createDataFrame(data, ['name', 'country'])

    countries = ['United Kingdom', 'Netherlands']
    filtered_df = filter_by_country(df, countries)

    expected_data = [
        ('Freddie Mercury', 'United Kingdom'),
        ('Eva Simons', 'Netherlands'),
        ('Alex Turner', 'United Kingdom')
    ]
    expected_df = spark.createDataFrame(expected_data, ['name', 'country'])

    assert_df_equality(filtered_df, expected_df, ignore_nullable=True)
