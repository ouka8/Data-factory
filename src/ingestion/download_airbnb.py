import os
import time
from kaggle.api.kaggle_api_extended import KaggleApi
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ingestion").getOrCreate()

DATASET = "dgomonov/new-york-city-airbnb-open-data"

# Get absolute path to ensure consistency across environments
BASE_PATH = os.path.dirname(os.path.abspath(__file__))
BRONZE_PATH = os.path.normpath(os.path.join(BASE_PATH, "../../data/bronze"))
BRONZE_PATH = os.path.abspath(BRONZE_PATH)

# S3 paths
S3_BRONZE_PATH = "s3a://amalam/bronze/airbnb/"

def download():
    """Download Airbnb dataset from Kaggle to local Bronze directory"""
    print("=" * 70)
    print("STEP 1: DOWNLOAD FROM KAGGLE")
    print("=" * 70)
    print(f"Downloading to: {BRONZE_PATH}\n")
    
    api = KaggleApi()
    api.authenticate()

    os.makedirs(BRONZE_PATH, exist_ok=True)

    print(" Downloading from Kaggle...")
    api.dataset_download_files(
        DATASET,
        path=BRONZE_PATH,
        unzip=True
    )
    
    time.sleep(1)

    print(f"\n Download complete. Files in local Bronze:")
    files = os.listdir(BRONZE_PATH)
    for f in files:
        full_path = os.path.join(BRONZE_PATH, f)
        if os.path.isfile(full_path):
            size_mb = os.path.getsize(full_path) / (1024 * 1024)
            print(f"   - {f:<30} ({size_mb:.2f} MB)")
        else:
            print(f"   - {f:<30} (directory)")


def push_to_s3():
    """Read CSV locally and write to S3 Bronze as single CSV file"""
    print("\n" + "=" * 70)
    print("STEP 2: UPLOAD TO S3 BRONZE (CSV FORMAT)")
    print("=" * 70)
    
    import pandas as pd
    
    file_path = os.path.join(BRONZE_PATH, "AB_NYC_2019.csv")
    
    print(f"\nLooking for: {file_path}")
    print(f"File exists: {os.path.exists(file_path)}")

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"CSV file not found: {file_path}")

    try:
        # Read with pandas
        print(f"\n Reading CSV with pandas...")
        pdf = pd.read_csv(file_path)
        print(f" Read {len(pdf)} rows, {len(pdf.columns)} columns")
        
        # Write directly to S3 with pandas (simple & clean)
        print(f"\n Writing to S3: {S3_BRONZE_PATH}AB_NYC_2019.csv")
        
        # Configure S3 access
        pdf.to_csv(
            f"{S3_BRONZE_PATH}AB_NYC_2019.csv",
            index=False,
            storage_options={'anon': False}
        )
        
        print(f" Successfully uploaded to S3 Bronze!")
        
    except Exception as e:
        print(f" Error: {e}")
        raise


if __name__ == "__main__":
    print("\n")

    print("║" + " " * 15 + "AIRBNB DATA INGESTION - ARCHITECT" + " " * 20 + "║")

    
    download()
    push_to_s3()
    
    print("\n" + "=" * 70)
    print(" INGESTION COMPLETE")
    print("=" * 70)
    print("\nData flow:")
    print("  Local Bronze: data/bronze/AB_NYC_2019.csv")
    print("  S3 Bronze:    s3a://amalam/bronze/airbnb/AB_NYC_2019.csv")
    print("=" * 70 + "\n")
