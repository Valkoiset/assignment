from pyspark.sql import DataFrame


def process_data(dataset_one: DataFrame, dataset_two: DataFrame, countries: list) -> DataFrame:
    """
    Processes two datasets by dropping specific columns, filtering by country, merging on a common identifier,
    and renaming columns according to a mapping.

    :param dataset_one: The first input DataFrame to process.
    :param dataset_two: The second input DataFrame to process.
    :param countries: A list of countries used to filter `dataset_one`.
    """
    dataset_one = drop_columns(dataset_one, ['first_name', 'last_name'])
    dataset_one = filter_by_country(dataset_one, countries)

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
    """
    Filters a PySpark DataFrame to include only the rows where the 'country' column values are in the specified list of countries.

    :param df: A PySpark DataFrame to filter.
    :param countries: A list of country names (strings) to include in the filtered DataFrame.
    :return: DataFrame containing only the rows where the 'country' value is in the provided list.
    """
    return df.filter(df.country.isin(countries))


def rename_columns(df: DataFrame, columns_map: dict) -> DataFrame:
    """
    Renames columns in a PySpark DataFrame based on a given mapping.

    :param df: The PySpark DataFrame whose columns you want to rename.
    :param columns_map: A dictionary with keys as current column names and values as new column names.
    :return: DataFrame with renamed columns.
    """
    for old_name, new_name in columns_map.items():
        df = df.withColumnRenamed(old_name, new_name)
    return df
