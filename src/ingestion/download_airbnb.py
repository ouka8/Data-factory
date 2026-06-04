import os
from kaggle.api.kaggle_api_extended import KaggleApi
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ingestion").getOrCreate()

DATASET = "dgomonov/new-york-city-airbnb-open-data"

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
BRONZE_PATH = os.path.normpath(os.path.join(BASE_PATH, "../../data/bronze"))

def download():
    api = KaggleApi()
    api.authenticate()

    os.makedirs(BRONZE_PATH, exist_ok=True)

    api.dataset_download_files(
        DATASET,
        path=BRONZE_PATH,
        unzip=True
    )

    print("Files:", os.listdir(BRONZE_PATH))


def push_to_s3():
    file_path = os.path.join(BRONZE_PATH, "AB_NYC_2019.csv")

    if not os.path.exists(file_path):
        raise Exception(f"File not found: {file_path}")

    df = spark.read.csv(file_path, header=True, inferSchema=True)

    df.write.mode("overwrite").parquet(
        "s3a://amalam/bronze/airbnb/raw/"
    )

    print("Uploaded to S3")


if __name__ == "__main__":
    download()
    push_to_s3()
