import csv
import json
import html
import os
import boto3
from dotenv import load_dotenv

csv.field_size_limit(2147483647)

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

def csv_to_osm(node_file_key, way_file_key, relation_file_key, output_file_key):
    # Explicitly provide AWS credentials
    load_dotenv()
    aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
    print(aws_access_key_id ,'check')
    aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')

    # Create S3 client with provided credentials
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
   
    # Read node file directly from S3
    node_response = s3.get_object(Bucket='touring-buddy', Key=node_file_key)
    node_content = node_response['Body'].iter_lines()

    # Read way file directly from S3
    way_response = s3.get_object(Bucket='touring-buddy', Key=way_file_key)
    way_content = way_response['Body'].iter_lines()

    # Read relation file directly from S3
    relation_response = s3.get_object(Bucket='touring-buddy', Key=relation_file_key)
    relation_content = relation_response['Body'].iter_lines()

    min_lat = float('inf')
    min_lon = float('inf')
    max_lat = float('-inf')
    max_lon = float('-inf')

    # Process node CSV
    node_reader = csv.reader((line.decode('utf-8') for line in node_content))
    next(node_reader)  # Skip the header line
    for line in node_reader:
        lat = float(line[1])
        lon = float(line[2])
        min_lat = min(min_lat, lat / 10**7)
        min_lon = min(min_lon, lon / 10**7)
        max_lat = max(max_lat, lat / 10**7)
        max_lon = max(max_lon, lon / 10**7)

    # Create the output OSM XML content
    osm_content = '<?xml version="1.0" encoding="UTF-8"?>\n'
    osm_content += '<osm version="0.6" generator="osmium/1.16.0">\n'
    osm_content += '  <bounds minlat="{:}" minlon="{:}" maxlat="{:}" maxlon="{:}"/>\n'.format(min_lat, min_lon, max_lat, max_lon)

    # Process node CSV
    node_reader = csv.reader((line.decode('utf-8') for line in node_content))
    next(node_reader)  # Skip the header line
    for line in node_reader:
        print(line,'node')
        osm_content += '  <node id="{}" version="1" timestamp="2024-03-15T00:00:00Z" lat="{:.7f}" lon="{:.7f}">\n'.format(line[0], float(line[1]) / 10**7, float(line[2]) / 10**7)
        try:
            tags = json.loads(line[3].replace('""', '"'))
            cleaned_tags = clean_tags(tags)
            for k, v in cleaned_tags.items():
                osm_content += '    <tag k="{}" v="{}"/>\n'.format(escape_xml(k), escape_xml(v))
        except json.JSONDecodeError:
            pass
        osm_content += '  </node>\n'
   
    # Process way CSV
    way_reader = csv.reader((line.decode('utf-8') for line in way_content))
    next(way_reader)  # Skip the header line
    for line in way_reader:
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

    # Process relation CSV
    rels_reader = csv.reader((line.decode('utf-8') for line in relation_content))
    next(rels_reader)  # Skip the header line
    for line in rels_reader:
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

    # Write the OSM content to S3 bucket
    s3.put_object(Body=osm_content.encode('utf-8'), Bucket='touring-buddy', Key=output_file_key)

# Example usage
csv_to_osm('india-nodes.csv', 'india-ways.csv', 'india-rels.csv', 'india.osm')

