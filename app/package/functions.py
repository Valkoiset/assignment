from pyspark.sql import DataFrame


def process_data(dataset_one, dataset_two, countries):
    dataset_one = drop_columns(dataset_one, ['first_name', 'last_name'])
    dataset_one = filter_by_country(dataset_one, ['United Kingdom', 'Netherlands'])

    dataset_two = drop_columns(dataset_two, ['cc_n'])

    df_merged = dataset_one.join(dataset_two, 'id', 'inner')

    columns_map = {
        'id': 'client_identifier',
        'btc_a': 'bitcoin_address',
        'cc_t': 'credit_card_type'
    }
    df_merged = rename_columns(df_merged, columns_map)
    return df_merged


def drop_columns(df: DataFrame, columns_to_remove: list) -> DataFrame:
    """
    Removes personal identifiable information from the dataset.

    :param df: Input DataFrame containing client information.
    :param columns_to_remove: A list of columns to drop from the DataFrame.
    :return: DataFrame with personal information columns removed.
    """
    df_cleaned = df.drop(*columns_to_remove)
    return df_cleaned


def filter_by_country(df: DataFrame, countries: list) -> DataFrame:
    return df.filter(df.country.isin(countries))


def rename_columns(df: DataFrame, columns_map: dict) -> DataFrame:
    """
    Renames columns in a PySpark DataFrame based on a given mapping.

    :param df: The PySpark DataFrame whose columns you want to rename.
    :param columns_map: A dictionary with keys as current column names and values as new column names.

    Returns:
    - DataFrame with renamed columns.
    """
    # Iterate over the dictionary and rename the columns
    for old_name, new_name in columns_map.items():
        df = df.withColumnRenamed(old_name, new_name)
    return df
