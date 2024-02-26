from pyspark.sql import DataFrame


def process_data(dataset_one, dataset_two, countries):
    # Implement the filtering, joining, and column renaming logic here
    pass


def filter_by_country(df: DataFrame, countries: list) -> DataFrame:
    return df.filter(df.country.isin(countries))
