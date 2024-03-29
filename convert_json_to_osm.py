import argparse
import json
import os
import logging
import boto3
from dotenv import load_dotenv
import xml.etree.ElementTree as ET
import ijson

def read_json_from_s3(bucket_name, json_key):
    try: 
        load_dotenv()
        aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
        aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
        if not aws_access_key_id or not aws_secret_access_key:
            logging.error("AWS credentials are not set properly.")
            return None
        
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        response = s3.get_object(Bucket=bucket_name, Key=json_key)
        with response['Body'] as json_data:
            data = json_data.read().decode('utf-8')
            json_data = json.loads(data)
            print(json_data)
        return json_data
    except Exception as e:
        logging.error(f"Error reading JSON data from S3: {e}")
        return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script description")
    parser.add_argument("--source", dest="source_file_name", required=True, help="Source file name")
    parser.add_argument("--destination", dest="destination_file_name", required=True, help="Destination file name")
    args = parser.parse_args()
    logging.info(f"STARTING FILE PROCESSING {args.source_file_name} {args.destination_file_name}")
    bucket_name = 'touring-buddy'
    json_key = args.source_file_name

    json_data = read_json_from_s3(bucket_name, json_key)
    if json_data:
        print("JSON data read successfully from S3:")
        print(json_data)
    else:
        print("Failed to read JSON data from S3.")
