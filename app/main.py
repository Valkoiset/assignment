from kommati_para.cleaning import process_data
from pyspark.sql import SparkSession
import argparse


def main(dataset_one_path: str, dataset_two_path: str, countries: list) -> None:
    spark = SparkSession.builder.appName('KommatiPara').getOrCreate()

    dataset_one = spark.read.csv(dataset_one_path, header=True)
    dataset_two = spark.read.csv(dataset_two_path, header=True)

    df_merged = process_data(dataset_one, dataset_two, countries)

    # It was not mentioned in exercise description whether it needs to be 'append' or 'overwrite', so
    # I assume the app should be running regularly on a scheduled basis and append new files instead of overwriting everything
    df_merged.coalesce(1).write.mode('append').csv('client_data/', header=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process some datasets.')
    parser.add_argument('--dataset_one_path', required=True, help='Path to the first dataset')
    parser.add_argument('--dataset_two_path', required=True, help='Path to the second dataset')
    parser.add_argument('--countries', nargs='+', help='List of countries')

    args = parser.parse_args()

    main(
        dataset_one_path=args.dataset_one_path,
        dataset_two_path=args.dataset_two_path,
        countries=args.countries
    )
