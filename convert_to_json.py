import argparse
import csv
import json
import os
import re
import boto3
from dotenv import load_dotenv
from tqdm import tqdm
import logging

CHUNK_SIZE = 8 * 1024 * 1024  # 8MB chunk size

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def csv_to_osm_json(row):
    way_id = int(row[0]) # Way id
    node_ids_str = row[1].strip() #  Nodes 
    tags_str = row[2].strip() # tags 
    
    node_ids =[]
    # Parse node IDs
    if(node_ids_str):
        input_node_string = node_ids_str.strip('{}')
    # Split the string by commas and convert each element to an integer
        node_ids = [int(num) for num in input_node_string.split(',')]
    # Parse tags
    tags = {}
    
    if(tags_str and tags_str != ''):
       tag_json = json.loads(tags_str)
       tags=tag_json
       
    way = {
        "type": "way",
        "id": way_id,
        "nodes": node_ids,
        "tags": tags
    }
    return way

def write_json_to_s3(json_data, bucket_name, file_key):
    load_dotenv()
    aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    if not aws_access_key_id or not aws_secret_access_key:
        logger.error("AWS credentials are not set properly.")
        return

    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    upload_large_object(s3, json.dumps(json_data).encode('utf-8'), bucket_name, file_key)

def convert_csv_to_osm_json_and_write_to_s3(bucket_name, csv_key, json_key):
    load_dotenv()
    aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    if not aws_access_key_id or not aws_secret_access_key:
        logger.error("AWS credentials are not set properly.")
        return
    
    osm_data = {"version": 0.6, "generator": "osmium/1.16.0"}
    osm_data["elements"] = []

    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    try:
        with s3.get_object(Bucket='touring-buddy', Key=csv_key)['Body'] as source_file:
            next(source_file)  # Skip the header line
        # Process each line in the CSV
            for line_num, row in tqdm(enumerate(csv.reader((line.decode('utf-8') for line in source_file))), desc="Processing Ways"):
                json = csv_to_osm_json(row=row)
                osm_data["elements"].append(json)
                
        write_json_to_s3(osm_data, bucket_name, json_key)
    except Exception as e:
        print(f"LINE NUMBER : {line_num}",row)
        logger.error(f"Error processing CSV file: {e}")


def upload_large_object(s3_client, body, bucket, key):
    # Initialize multipart upload
    response = s3_client.create_multipart_upload(Bucket=bucket, Key=key)
    upload_id = response['UploadId']
    part_number = 1
    parts = []

    try:
        # Upload parts
        for offset in range(0, len(body), CHUNK_SIZE):
            chunk = body[offset:offset + CHUNK_SIZE]
            part = s3_client.upload_part(
                Bucket=bucket,
                Key=key,
                PartNumber=part_number,
                UploadId=upload_id,
                Body=chunk
            )
            parts.append({'PartNumber': part_number, 'ETag': part['ETag']})
            part_number += 1

        # Complete multipart upload
        s3_client.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts}
        )
    except Exception as e:
        # Abort multipart upload on failure
        s3_client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
        logger.error(f"Error uploading object to S3: {e}")
        raise e


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script description")
    parser.add_argument("--source", dest="source_file_name", required=True, help="Source file name")
    parser.add_argument("--destination", dest="destination_file_name", required=True, help="Destination file name")
    args = parser.parse_args()
    logger.info(f"STARTING FILE PROCESSING {args.source_file_name} {args.destination_file_name}")
    bucket_name = 'touring-buddy'
    csv_file_key = args.source_file_name
    json_file_key = args.destination_file_name
    convert_csv_to_osm_json_and_write_to_s3(bucket_name, csv_file_key, json_file_key)
