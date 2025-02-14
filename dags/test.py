#/Users/hishitathakkar/Desktop/sec-financial-data
import io
import os
import zipfile
import boto3
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import requests


'''def upload_file_to_s3(file_content, bucket_name, s3_path):

    #load_dotenv()
    load_dotenv(r'/Users/hishitathakkar/Desktop/sec-financial-data/s3_access.env')

    #s3 = boto3.client('s3')
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_REGION')
    )

    s3.upload_fileobj(file_content, bucket_name, s3_path)
'''

def finance_data_scrapper(url, bucket_name,req_folder):


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
             
            foldername = href.split('/')[-1]
            if req_folder in foldername:
               
                try:
                    # Stream the file
                    file_response = requests.get(href, stream=True, headers=headers)

                    if file_response.status_code == 200:

                        #Create BytesIO object to hold zip content in memory
                        zip_buffer = io.BytesIO(file_response.content)

                        # Get filename
                        main_file_name = link.get('download') or href.split('/')[-1]
                        base, extension = os.path.splitext(main_file_name)

                        try:

                            # Extract and upload each file from zip
                            with zipfile.ZipFile(zip_buffer) as zip_ref:

                                for file_name in zip_ref.namelist():
                                    # Read file content from zip
                                    file_content = zip_ref.read(file_name)

                                    # Upload individual file to S3
                                    file_buffer = io.BytesIO(file_content)

                                    filepath = f'importFiles/{base}/{file_name}'
                                    #upload_file_to_s3(file_buffer, bucket_name, filepath)

                                    s3_hook = S3Hook(aws_conn_id='aws_default')
                                    s3_hook.get_conn().upload_fileobj(file_buffer, bucket_name, filepath)

                                    print(f'Uploaded {base}/{file_name} to S3 bucket {bucket_name}')

                        except zipfile.BadZipFile:
                            print("Error: Invalid ZIP file format")

                    else:
                        print(f'Failed to download zip file: {file_response.status_code}')


                except Exception as e:
                    print(f"Error while getting response {href}: {e}")



#finance_data_link = 'https://www.sec.gov/data-research/sec-markets-data/financial-statement-data-sets'
#bucket_name = 'assignment2bigdatasystemss'


#result = finance_data_scrapper(finance_data_link, bucket_name)
#print(f'Scrapping and Uploading of files to s3 {bucket_name} complete.')
