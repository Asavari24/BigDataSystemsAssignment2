import pandas as pd
from io import StringIO
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import logging

# AWS S3 Configuration
S3_BUCKET_NAME = "assignment2bigdatasystemss"
S3_EXTRACTED_FOLDER = "importFiles/"
S3_TRANSFORMED_FOLDER = "sec-transformed-files/"

COLUMNS_MAP = {
    "sub.txt": ["adsh", "cik", "name", "sic", "countryba", "stprba", "cityba", "zipba",
                "bas1", "bas2", "baph", "countryma", "stprma", "cityma", "zipma",
                "mas1", "mas2", "countryinc", "stprinc", "ein", "former", "changed",
                "afs", "wksi", "fye", "form", "period", "filed", "prevrpt",
                "detail", "instance"],
    "num.txt": ["adsh", "tag", "version", "coreg", "ddate", "qtrs", "uom", "value"],
}

def process_file(bucket_name, s3_key):
    """Download, transform, and upload the .txt file to S3 as a CSV."""
    s3_hook = S3Hook(aws_conn_id="aws_default")
    file_name = s3_key.split("/")[-1]
    quarter_folder = s3_key.split("/")[1]  # Extract quarter name (e.g., 2016q4)

    logging.info(f"Processing {file_name} from {quarter_folder}...")

    try:
        file_content = s3_hook.read_key(bucket_name=bucket_name, key=s3_key)

        if file_name in COLUMNS_MAP:
            col_names = COLUMNS_MAP[file_name]
            df = pd.read_csv(StringIO(file_content), sep="\t", names=col_names, dtype=str)
        else:
            logging.warning(f"Skipping {file_name} (unknown format)")
            return

        # ✅ Convert date columns to correct format
        if "period" in df.columns:
            df["period"] = pd.to_datetime(df["period"], errors="coerce").dt.strftime("%Y-%m-%d")
        if "ddate" in df.columns:
            df["ddate"] = pd.to_datetime(df["ddate"], errors="coerce").dt.strftime("%Y-%m-%d")

        # Save transformed data to CSV
        transformed_csv_key = f"{S3_TRANSFORMED_FOLDER}{quarter_folder}/{file_name.replace('.txt', '.csv')}"
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        # ✅ Upload to S3
        s3_hook.load_string(string_data=csv_buffer.getvalue(), key=transformed_csv_key, bucket_name=bucket_name, replace=True)
        logging.info(f"✅ Transformed and uploaded: {transformed_csv_key}")

    except Exception as e:
        logging.error(f"Error processing file {file_name}: {e}")
