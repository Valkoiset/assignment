from package.functions import process_data
from pyspark.sql import SparkSession


def main(dataset_one_path: str, dataset_two_path, countries):
    spark = SparkSession.builder.appName('KommatiPara').getOrCreate()

    dataset_one = spark.read.csv(dataset_one_path, header=True)
    dataset_two = spark.read.csv(dataset_two_path, header=True)

    processed_df = process_data(dataset_one, dataset_two, countries)
    processed_df.show()

    # processed_df.coalesce(1).write.csv('../client_data/result', header=True)


if __name__ == '__main__':
    main(
        dataset_one_path='../input_data/dataset_one.csv',
        dataset_two_path='../input_data/dataset_one.csv',
        countries=['United Kingdom', 'Netherlands']
    )
