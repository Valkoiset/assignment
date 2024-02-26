from pyspark.sql import DataFrame


def process_data(dataset_one, dataset_two, countries):
    dataset_one = drop_columns(dataset_one, ['first_name', 'last_name'])
    dataset_one = filter_by_country(dataset_one, ['United Kingdom', 'Netherlands'])
    return dataset_one


def drop_columns(df: DataFrame, columns_to_remove: list) -> DataFrame:
    """
    Removes personal identifiable information from the dataset.

    :param df: Input DataFrame containing client information.
    :param columns_to_remove: list of columns to drop from the DataFrame.
    :return: DataFrame with personal information columns removed.
    """
    df_cleaned = df.drop(*columns_to_remove)
    return df_cleaned


def filter_by_country(df: DataFrame, countries: list) -> DataFrame:
    return df.filter(df.country.isin(countries))
