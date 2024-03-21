
import csv
import json
import html
import os
import boto3
from dotenv import load_dotenv
import argparse
from enums import FileName


csv.field_size_limit(2147483647)


min_lat = float('inf')
min_lon = float('inf')
max_lat = float('-inf')
max_lon = float('-inf')


osm_content = '<?xml version="1.0" encoding="UTF-8"?>\n'
osm_content += '<osm version="0.6" generator="osmium/1.16.0">\n'
osm_content += '  <bounds minlat="{:}" minlon="{:}" maxlat="{:}" maxlon="{:}"/>\n'.format(min_lat, min_lon, max_lat, max_lon)



def clean_tags(tags):
    cleaned_tags = {}
    for k, v in tags.items():
        if isinstance(v, int):  # Check if the value is an integer
            v = str(v)  # Convert integer value to string
        cleaned_value = html.escape(v.strip())
        cleaned_tags[k] = cleaned_value
    return cleaned_tags

def escape_xml(text):
    return html.escape(text, quote=False)


def process_nodes(sourceFileName,destinationFileName,aws_access_key_id,aws_secret_access_key):
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    # Read node file directly from S3
    node_response = s3.get_object(Bucket='touring-buddy', Key=sourceFileName)
    node_content = node_response['Body'].iter_lines()
    next(node_content)  # Skip the header line
    

    # Process node CSV
    for line in csv.reader((line.decode('utf-8') for line in node_content)):
        print(line,'node')
        lat = float(line[1])
        lon = float(line[2])
        min_lat = min(min_lat, lat / 10**7)
        min_lon = min(min_lon, lon / 10**7)
        max_lat = max(max_lat, lat / 10**7)
        max_lon = max(max_lon, lon / 10**7)
    
    osm_content += '</osm>\n'
    s3.put_object(Body=osm_content.encode('utf-8'), Bucket='touring-buddy', Key=destinationFileName)



def process_way(sourceFileName,destinationFileName,aws_access_key_id,aws_secret_access_key):
    
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
      # Create the output OSM XML content
   
    # Read way file directly from S3 in chunks
    way_response = s3.get_object(Bucket='touring-buddy', Key=sourceFileName)
    way_content = way_response['Body'].iter_lines()
    next(way_content)  # Skip the header line

    # Process way CSV
    for line in csv.reader((line.decode('utf-8') for line in way_content)):
        print(line,'way')
        osm_content += '  <way id="{}" version="1" timestamp="2024-03-15T00:00:00Z">\n'.format(line[0])
        nodes = line[1].strip('"{}').split(',')
        for node in nodes:
            osm_content += '    <nd ref="{}"/>\n'.format(node.strip())
        try:
            tags = json.loads(line[2].replace('""', '"'))
            cleaned_tags = clean_tags(tags)
            for k, v in cleaned_tags.items():
                osm_content += '    <tag k="{}" v="{}"/>\n'.format(escape_xml(k), escape_xml(v))
        except json.JSONDecodeError:
            pass
        osm_content += '  </way>\n'
        osm_content += '</osm>\n'
    s3.put_object(Body=osm_content.encode('utf-8'), Bucket='touring-buddy', Key=destinationFileName)



def process_relation(sourceFileName,destinationFileName,aws_access_key_id,aws_secret_access_key):
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    relation_response = s3.get_object(Bucket='touring-buddy', Key=sourceFileName)
    relation_content = relation_response['Body'].iter_lines()
    next(relation_content)  # Skip the header line

    # Process relation CSV
    for line in csv.reader((line.decode('utf-8') for line in relation_content)):
        print(line,'rels')
        osm_content += '  <relation id="{}" version="1" timestamp="2024-03-15T00:00:00Z">\n'.format(line[0])
        try:
            members = json.loads(line[1].replace('""', '"'))
            for member in members:
                member_type = member["type"].lower()
                if member_type == 'w':
                    member_type = 'way'
                elif member_type == 'n':
                    member_type = 'node'
                elif member_type == 'r':
                    member_type = 'relation'
                osm_content += '    <member type="{}" ref="{}" role="{}"/>\n'.format(member_type, member["ref"], escape_xml(member["role"]))
        except json.JSONDecodeError:
            pass
        try:
            tags = json.loads(line[2].replace('""', '"'))
            cleaned_tags = clean_tags(tags)
            for k, v in cleaned_tags.items():
                osm_content += '    <tag k="{}" v="{}"/>\n'.format(escape_xml(k), escape_xml(v))
        except json.JSONDecodeError:
            pass
        osm_content += '  </relation>\n'

    osm_content += '</osm>\n'

    s3.put_object(Body=osm_content.encode('utf-8'), Bucket='touring-buddy', Key=destinationFileName)
    
    
def main(sourceFileName, destinationFileName):
    load_dotenv()
    aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
    print(aws_access_key_id, 'check')
    aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')

    if sourceFileName == FileName.NODE_FILE:
        process_nodes(sourceFileName, destinationFileName, aws_access_key_id, aws_secret_access_key)
    elif sourceFileName == FileName.RELATION_FILE:
        process_relation(sourceFileName, destinationFileName, aws_access_key_id, aws_secret_access_key)
    else:
        process_way(sourceFileName, destinationFileName, aws_access_key_id, aws_secret_access_key)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script description")
    parser.add_argument("--source", dest="source_file_name", required=True, help="Source file name")
    parser.add_argument("--destination", dest="destination_file_name", required=True, help="Destination file name")
    args = parser.parse_args()

    main(sourceFileName=args.source_file_name,destinationFileName= args.destination_file_name)