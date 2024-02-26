from package.functions import filter_by_country, process_data
from pyspark.sql import SparkSession


def main(dataset_one_path: str, dataset_two_path, countries):
    spark = SparkSession.builder.appName('KommatiPara').getOrCreate()

    dataset_one = spark.read.csv(dataset_one_path, header=True)
    dataset_two = spark.read.csv(dataset_two_path, header=True)

    # processed_df = process_data(dataset_one, dataset_two, countries)
    dataset_one_filtered = filter_by_country(dataset_one, countries)
    dataset_one_filtered.show()
    
    # processed_df.write.csv('client_data/result.csv', header=True)


if __name__ == '__main__':
    main(
        dataset_one_path='../input_data/dataset_one.csv',
        dataset_two_path='../input_data/dataset_one.csv',
        countries=['United Kingdom', 'Netherlands']
    )

    # spark = SparkSession.builder.appName('KommatiPara').getOrCreate()
    #
    # dataset_one = spark.read.csv('../input_data/dataset_one.csv', header=True)
    # dataset_two = spark.read.csv('../input_data/dataset_one.csv', header=True)
    #
    # df = filter_by_country(dataset_one, ['United Kingdom', 'Netherlands'])
    # df.show()
