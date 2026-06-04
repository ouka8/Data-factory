import os
from kaggle.api.kaggle_api_extended import KaggleApi

DATASET = "dgomonov/new-york-city-airbnb-open-data"

def download_airbnb():
    print("Starting Airbnb ingestion from Kaggle...")

    # Auth Kaggle
    api = KaggleApi()
    api.authenticate()

    # Dossier Bronze
    bronze_path = "data/bronze"
    os.makedirs(bronze_path, exist_ok=True)

    # Download + unzip
    api.dataset_download_files(
        DATASET,
        path=bronze_path,
        unzip=True
    )

    print("Data downloaded into Bronze zone")

    # check files
    print("Files:", os.listdir(bronze_path))

if __name__ == "__main__":
    download_airbnb()
