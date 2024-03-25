import csv
import json
import re
from tqdm import tqdm 
import html
import os
import boto3
import argparse
import logging
from dotenv import load_dotenv

if 'PYCHARM_HOSTED' not in os.environ:
    from tqdm import tqdm


# Configure logging
logging.basicConfig(filename='error.log', level=logging.ERROR)

csv.field_size_limit(2147483647)

def clean_tags(tags):
    cleaned_tags = {}
    for k, v in tags.items():
        try:
            if isinstance(v, int):  # Check if the value is an integer
                v = str(v)  # Convert integer value to string
            cleaned_value = html.escape(v.strip())
            cleaned_tags[k] = cleaned_value
        except Exception as e:
            logging.error(f"Error cleaning tag: {e}")
    return cleaned_tags

def escape_xml(text):
    return html.escape(text, quote=False)

import logging
import csv
import json
import boto3
from tqdm import tqdm

def process_nodes(sourceFileName, destinationFileName, aws_access_key_id, aws_secret_access_key):
    try:
        # Initialize logging
        logging.basicConfig(filename='error.log', level=logging.ERROR)

        # Initialize S3 client
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        print(f"S3 INITIALIAZE {s3}")

        # Retrieve node data from S3
        node_response = s3.get_object(Bucket='touring-buddy', Key=sourceFileName)
        node_content = node_response['Body'].iter_lines()
        next(node_content)  # Skip the header line

        # Initialize XML content
        osm_content = '<?xml version="1.0" encoding="UTF-8"?>\n'
        osm_content += '<osm version="0.6" generator="osmium/1.16.0">\n'

        # Initialize bounds
        min_lat = float('inf')
        min_lon = float('inf')
        max_lat = float('-inf')
        max_lon = float('-inf')

        # Convert generator to list to get total line count
        node_lines = list(node_content)
        total_iterations = len(node_lines)

        # Process each line in the CSV
        for line_num, line in tqdm(enumerate(csv.reader((line.decode('utf-8') for line in node_lines))), total=total_iterations, desc="Processing nodes", disable='PYCHARM_HOSTED' in os.environ):
            print('processing nodes:',line_num)
            try:
                # Extract node information
                node_id = line[0]
                lat = float(line[1]) / 10**7
                lon = float(line[2]) / 10**7
                
                # Update bounds
                min_lat = min(min_lat, lat)
                min_lon = min(min_lon, lon)
                max_lat = max(max_lat, lat)
                max_lon = max(max_lon, lon)
                
                # Process tags if available
                if len(line) > 3 and line[3]:
                    tags_str = re.sub(r'(?<!\\)""', '"', line[3].replace('""', '\\"'))# Replace double double-quotes with single double-quotes using replace
                    tags_str = re.sub(r'(?<!\\)""', '"', tags_str)  # Further replace any remaining double double-quotes
                    tags = json.loads(tags_str)  # Load the JSON string into a dictionary
                    cleaned_tags = clean_tags(tags)
                    osm_content += '  <node id="{}" version="1" timestamp="2024-03-15T00:00:00Z" lat="{:.7f}" lon="{:.7f}">\n'.format(node_id, lat, lon)
                    for k, v in cleaned_tags.items():
                        osm_content += '    <tag k="{}" v="{}"/>\n'.format(escape_xml(k), escape_xml(v))
                    osm_content += '  </node>\n'
                else:
                    osm_content += '  <node id="{}" version="1" timestamp="2024-03-15T00:00:00Z" lat="{:.7f}" lon="{:.7f}"/>\n'.format(node_id, lat, lon)
            except Exception as e:
                # Log error with line number
                logging.error(f"Error processing node at line {line_num + 1}: {e}")

        # Add bounds to the XML content
        osm_content += '<bounds minlat="{:.7f}" minlon="{:.7f}" maxlat="{:.7f}" maxlon="{:.7f}"/>\n'.format(min_lat, min_lon, max_lat, max_lon)
        osm_content += '</osm>\n'
        print(f"____________________PROCESSING_COMPLETED_______________________________")
        print(f"TOTAL RECORDS :- {node_lines}")
        print(f"INITIATING UPLOAD OSM FILE TO S3 {destinationFileName}")
        s3.put_object(Body=osm_content.encode('utf-8'), Bucket='touring-buddy', Key=destinationFileName)
        print(f"DONE UPLOADEING {destinationFileName} TO S3")
        print(f"OSM CONTENT {osm_content.encode('utf-8')}")
    except Exception as e:
        # Log any exception occurred during the process
        print(f"Error processing nodes data: {e}")

def process_way(sourceFileName, destinationFileName, aws_access_key_id, aws_secret_access_key):
    try:
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        print(f"S3 INITIALIAZE {s3}")
        way_response = s3.get_object(Bucket='touring-buddy', Key=sourceFileName)
        way_content = way_response['Body'].iter_lines()
        next(way_content)  # Skip the header line

        osm_content = '<?xml version="1.0" encoding="UTF-8"?>\n'
        osm_content += '<osm version="0.6" generator="osmium/1.16.0">\n'

        # Store lines in a list
        way_lines = list(way_content)

        # Process way CSV
        total_iterations = len(way_lines)
        for line_num, line in tqdm(enumerate(csv.reader((line.decode('utf-8') for line in way_lines))), total=total_iterations, desc="Processing ways", disable='PYCHARM_HOSTED' in os.environ):
            print('processing ways:',line_num)
            osm_content += '  <way id="{}" version="1" timestamp="2024-03-15T00:00:00Z">\n'.format(line[0])
            nodes = line[1].strip('"{}').split(',')
            for node in nodes:
                osm_content += '    <nd ref="{}"/>\n'.format(node.strip())
            try:
                # Check if the third column contains valid JSON
                if line[2]:
                    tags = json.loads(line[2].replace('""', '"'))
                    cleaned_tags = clean_tags(tags)
                    for k, v in cleaned_tags.items():
                        osm_content += '    <tag k="{}" v="{}"/>\n'.format(escape_xml(k), escape_xml(v))
            except json.JSONDecodeError as e:
                # Log error with line number and continue to the next line
                continue
            except Exception as e:
                # Log other exceptions and continue to the next line
                print(f"Error processing way at line {line_num + 1}: {e}")
                continue
            osm_content += '  </way>\n'
        osm_content += '</osm>\n'
        print(f"____________________PROCESSING_COMPLETED_______________________________")
        print(f"TOTAL RECORDS :- {way_lines}")
        print(f"INITIATING UPLOAD OSM FILE TO S3 {destinationFileName}")
        s3.put_object(Body=osm_content.encode('utf-8'), Bucket='touring-buddy', Key=destinationFileName)
        print(f"DONE UPLOADEING {destinationFileName} TO S3")
        print(f"OSM CONTENT {osm_content.encode('utf-8')}")
    except Exception as e:
        print(f"Error processing ways: {e}")


def process_relation(sourceFileName, destinationFileName, aws_access_key_id, aws_secret_access_key):
    try:
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        print(f"S3 INITIALIAZE {s3}")
        relation_response = s3.get_object(Bucket='touring-buddy', Key=sourceFileName)
        relation_content = relation_response['Body'].iter_lines()
        next(relation_content)  # Skip the header line
        
        osm_content = '<?xml version="1.0" encoding="UTF-8"?>\n'
        osm_content += '<osm version="0.6" generator="osmium/1.16.0">\n'

        # Store lines in a list
        relation_lines = list(relation_content)

        # Process relation CSV
        total_iterations = len(relation_lines)
        for line_num, line in tqdm(enumerate(csv.reader((line.decode('utf-8') for line in relation_lines))), total=total_iterations, desc="Processing relation", disable='PYCHARM_HOSTED' in os.environ):
            print('processing relations:',line_num)
            osm_content += '  <relation id="{}" version="1" timestamp="2024-03-15T00:00:00Z">\n'.format(line[0])
            try:
                members = json.loads(line[1])
                for member in members:
                    member_type = member["type"].lower()
                    if member_type == 'w':
                        member_type = 'way'
                    elif member_type == 'n':
                        member_type = 'node'
                    elif member_type == 'r':
                        member_type = 'relation'
                    osm_content += '    <member type="{}" ref="{}" role="{}"/>\n'.format(member_type, member["ref"], escape_xml(member["role"]))
            except Exception as e:
                logging.error(f"Error processing relation members: {e}")
                pass
            
            try:
                tags = json.loads(line[2])
                cleaned_tags = clean_tags(tags)
                for k, v in cleaned_tags.items():
                    osm_content += '    <tag k="{}" v="{}"/>\n'.format(escape_xml(k), escape_xml(v))
            except Exception as e:
                print(f"Error processing relation tags: {e}")
                pass
            
            osm_content += '  </relation>\n'

        osm_content += '</osm>\n'
        print(f"____________________PROCESSING_COMPLETED_______________________________")
        print(f"TOTAL RECORDS :- {relation_lines}")
        print(f"INITIATING UPLOAD OSM FILE TO S3 {destinationFileName}")
        s3.put_object(Body=osm_content.encode('utf-8'), Bucket='touring-buddy', Key=destinationFileName)
        print(f"DONE UPLOADEING {destinationFileName} TO S3")
        print(f"OSM CONTENT {osm_content.encode('utf-8')}")
    except Exception as e:
        logging.error(f"Error processing relations: {e}")

def main(sourceFileName, destinationFileName):
    load_dotenv()
    aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    print(f"LOADED ENV SUCCESSFULLY")
    # Determine the type of file based on the file name
    if sourceFileName == 'india-node.csv':
        print(f"EXECUTING process_nodes FUNCTION ")
        process_nodes(sourceFileName, destinationFileName, aws_access_key_id, aws_secret_access_key)
    elif sourceFileName == 'india-ways.csv':
        print(f"EXECUTING process_way FUNCTION ")
        process_way(sourceFileName, destinationFileName, aws_access_key_id, aws_secret_access_key)
    elif sourceFileName == 'india-rels.csv':
        print(f"EXECUTING process_relation FUNCTION ")
        process_relation(sourceFileName, destinationFileName, aws_access_key_id, aws_secret_access_key)
    else:
        logging.error("Invalid source file type.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script description")
    parser.add_argument("--source", dest="source_file_name", required=True, help="Source file name")
    parser.add_argument("--destination", dest="destination_file_name", required=True, help="Destination file name")
    args = parser.parse_args()
    print("STARTING FILE PROCESSING")
    main(sourceFileName=args.source_file_name, destinationFileName=args.destination_file_name)