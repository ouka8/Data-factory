import os
from kaggle.api.kaggle_api_extended import KaggleApi

DATASET = "dgomonov/new-york-city-airbnb-open-data"

def download():
    print("Starting Kaggle ingestion...")

    api = KaggleApi()
    api.authenticate()

    os.makedirs("data/raw", exist_ok=True)

    # download + unzip
    api.dataset_download_files(
        DATASET,
        path="data/raw",
        unzip=True
    )

    print("Download finished")

    files = os.listdir("data/raw")
    print("Files:", files)

if __name__ == "__main__":
    download()
