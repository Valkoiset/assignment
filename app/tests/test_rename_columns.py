from chispa.dataframe_comparer import assert_df_equality
# from kommati_para.cleaning import rename_columns
from pyspark.sql import SparkSession
import pytest
from ..package.cleaning import rename_columns


@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder.appName('TestRenameColumns').getOrCreate()


def test_rename_columns(spark):
    data = [('Anthony Kiedis', 61), ('John Frusciante', 53)]
    df = spark.createDataFrame(data, ['name', 'age'])

    columns_map = {'name': 'full_name', 'age': 'years'}
    renamed_df = rename_columns(df, columns_map)
    expected_df = spark.createDataFrame(data, ['full_name', 'years'])

    assert_df_equality(renamed_df, expected_df, ignore_nullable=True)
