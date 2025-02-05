import os
import boto3
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import requests


def upload_file_to_s3(file_content, bucket_name, s3_path):

    #load_dotenv()
    load_dotenv(r'C:\Users\Admin\Desktop\MS Data Architecture and Management\DAMG 7245 - Big Data Systems and Intelligence Analytics\Assignment 2\environment\s3_access.env')

    #s3 = boto3.client('s3')
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_REGION')
    )

    s3.upload_fileobj(file_content, bucket_name, s3_path)


def finance_data_scrapper(url, bucket_name):


    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
        'Cache-Control': 'max-age=0'
    }
    
    # Get the webpage content
    response1 = requests.get(url, headers=headers)
    soup = BeautifulSoup(response1.content, 'html.parser')


    # Find all links
    for link in soup.find_all('a'):
        
        href = link.get('href')

        if href.lower().endswith('.zip'):

            # Handle relative URLs
            if not href.startswith(('http://', 'https://')):
                href = requests.compat.urljoin('https://www.sec.gov', href)
            
            try:
                # Stream the file
                file_response = requests.get(href, stream=True)

                # Get filename
                filename = link.get('download') or href.split('/')[-1]

                filepath = f'importFiles/{filename}'
                data_path = upload_file_to_s3(file_response.raw, bucket_name, filepath)


            except Exception as e:
                print(f"Error downloading {href}: {e}")


    return data_path


finance_data_link = 'https://www.sec.gov/data-research/sec-markets-data/financial-statement-data-sets'
bucket_name = 'bigdatasystems2'


result = finance_data_scrapper(finance_data_link, bucket_name)
print(f'Scrapping and Uploading of files to s3 {bucket_name} complete.')
