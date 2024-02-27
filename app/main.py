from package.logging import setup_logging
from package.cleaning import process_data
from pyspark.sql import SparkSession


def main(dataset_one_path: str, dataset_two_path: str, countries: list) -> None:
    logger = setup_logging()

    spark = SparkSession.builder.appName('KommatiPara').getOrCreate()

    logger.info('SparkSession created')

    dataset_one = spark.read.csv(dataset_one_path, header=True)
    dataset_two = spark.read.csv(dataset_two_path, header=True)

    df_merged = process_data(dataset_one, dataset_two, countries)

    # Not mentioned in exercise description whether it needs to be 'append' or 'overwrite'
    # So I assume the app should be running regularly and append new files instead of overwriting everything
    df_merged.coalesce(1).write.mode('append').csv('../client_data/', header=True)


if __name__ == '__main__':
    main(
        dataset_one_path='../input_data/dataset_one.csv',
        dataset_two_path='../input_data/dataset_two.csv',
        countries=['United Kingdom', 'Netherlands']
    )
