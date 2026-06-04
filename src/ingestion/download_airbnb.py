import os
from kaggle.api.kaggle_api_extended import KaggleApi
from pyspark.sql import SparkSession

DATASET = "dgomonov/new-york-city-airbnb-open-data"

def download_airbnb():
    print("Starting Airbnb ingestion from Kaggle...")

    api = KaggleApi()
    api.authenticate()

    bronze_path = "data/bronze/airbnb/raw"
    os.makedirs(bronze_path, exist_ok=True)

    api.dataset_download_files(
        DATASET,
        path=bronze_path,
        unzip=True
    )

    print("Data downloaded into Bronze zone")
    print("Files:", os.listdir(bronze_path))

    return bronze_path


def push_to_s3(local_path):
    print("Starting Spark upload to S3...")

    spark = SparkSession.builder.getOrCreate()

    file_path = os.path.join(local_path, "AB_NYC_2019.csv")

    df = spark.read.csv(file_path, header=True, inferSchema=True)

    df.write.mode("overwrite").parquet(
        "s3a://amalam/bronze/airbnb/raw/"
    )

    print("Data written to S3 bronze layer")


if __name__ == "__main__":
    path = download_airbnb()
    push_to_s3(path)
