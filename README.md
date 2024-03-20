# CSV to OSM Converter

This script converts CSV files containing OpenStreetMap (OSM) data into OSM XML format. It retrieves the CSV files from an AWS S3 bucket, processes them, and then uploads the resulting OSM XML file back to the bucket.

## Prerequisites

Before running the script, ensure you have the following:

- Python 3 installed on your machine.
- Boto3 library installed (`pip install boto3`).

## Usage

1. Modify the script's configuration:

   - Replace `aws_access_key_id` and `aws_secret_access_key` with your AWS credentials.
   - Adjust the bucket name and file keys according to your S3 bucket structure.

2. Run the script:
   ```bash
   python csv_to_osm_converter.py
   ```

## Important Notes

- This script assumes that the CSV files follow a specific format:

  - Node CSV: Contains columns for node ID, latitude, longitude, and tags in JSON format.
  - Way CSV: Contains columns for way ID, nodes (list of node IDs), and tags in JSON format.
  - Relation CSV: Contains columns for relation ID, members (list of members with type, ref, and role), and tags in JSON format.

- The script uses the `csv`, `json`, `html`, and `boto3` Python libraries. Ensure they are installed.

- The resulting OSM XML file is uploaded back to the specified S3 bucket with the provided output file key.

## Limitations

- This script does not handle all possible edge cases of OSM data. Ensure your CSV files are well-formatted and compatible with the script's expectations.
