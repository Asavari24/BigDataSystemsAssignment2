import os
import io
import zipfile
import requests
from bs4 import BeautifulSoup
from airflow.providers.amazon.aws.hooks.s3 import S3Hook  

SEC_BASE_URL = "https://www.sec.gov"

def upload_file_to_s3(file_content, s3_path):
    """Uploads a file content (bytes) to S3."""
    try:
        file_buffer = io.BytesIO(file_content)
        s3_hook = S3Hook(aws_conn_id='aws_default')
        print(f"Uploading to S3: {s3_path}")
        s3_hook.get_conn().upload_fileobj(file_buffer, 'assignment2bigdatasystems', s3_path)
        print(f"Uploaded successfully: {s3_path}")
    except Exception as e:
        print(f"Error uploading to S3: {e}")

def fetch_and_upload_q4_2016_zip(source_url, bucket_name):
    """Fetches the SEC page, downloads Q4 2016 ZIP, extracts and uploads its contents to S3."""
    print(f"Fetching data from {source_url}...")  
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
        'Cache-Control': 'max-age=0'
    }

    try:
        response = requests.get(source_url, headers=headers)
        if response.status_code != 200:
            print(f"Failed to fetch webpage: {response.status_code}")
            return
        
        page_content = BeautifulSoup(response.content, 'html.parser')
        
        for anchor in page_content.find_all('a'):
            file_link = anchor.get('href')
            if file_link and "2016q4.zip" in file_link.lower():
                zip_url = SEC_BASE_URL + file_link
                print(f"Downloading ZIP file: {zip_url}")

                zip_response = requests.get(zip_url, headers=headers)
                if zip_response.status_code == 200:
                    zip_content = io.BytesIO(zip_response.content)
                    print("Extracting ZIP contents...")
                    
                    with zipfile.ZipFile(zip_content, 'r') as zip_ref:
                        for file_name in zip_ref.namelist():
                            with zip_ref.open(file_name) as extracted_file:
                                file_data = extracted_file.read()
                                s3_file_path = f"extracted/2016q4/{file_name}"  
                                upload_file_to_s3(file_data, s3_file_path)

                    print("All files extracted and uploaded successfully.")
                else:
                    print(f"Failed to download ZIP: {zip_url}, Status: {zip_response.status_code}")

                return  # Stop after processing the required file

        print("q4_2016.zip not found.")

    except Exception as e:
        print(f"Error while fetching data: {e}")

# Run script
if __name__ == "__main__":
    data_url = 'https://www.sec.gov/data-research/sec-markets-data/financial-statement-data-sets'
    s3_bucket = 'assignment2bigdatasystems' 
   
    print("Starting the script...")
    fetch_and_upload_q4_2016_zip(data_url, s3_bucket)
    print(f'Process completed. Files uploaded to S3 bucket: {s3_bucket}')
