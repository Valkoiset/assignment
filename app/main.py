from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.master('local').appName('assignment').getOrCreate()

    dataset_one = spark.read.csv('../input_data/dataset_one.csv')
    dataset_two = spark.read.csv('../input_data/dataset_one.csv')

    dataset_one.show()
