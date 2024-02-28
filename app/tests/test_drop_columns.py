from chispa.dataframe_comparer import assert_df_equality
# from kommati_para.cleaning import drop_columns
from pyspark.sql import SparkSession
import pytest
import sys

sys.path.append('../package')
from cleaning import drop_columns


@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder.appName('TestDropColumns').getOrCreate()


def test_drop_columns(spark):
    data = [
        ('Anthony Kiedis', 61, 'Michigan'),
        ('John Frusciante', 53, 'New York')
    ]
    df = spark.createDataFrame(data, ['name', 'age', 'state'])

    columns_to_remove = ['age', 'state']
    cleaned_df = drop_columns(df, columns_to_remove)

    expected_data = [('Anthony Kiedis',), ('John Frusciante',)]
    expected_df = spark.createDataFrame(expected_data, ['name'])

    assert_df_equality(cleaned_df, expected_df, ignore_nullable=True)
