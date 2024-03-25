import pandas as pd
import os

def split_csv(input_file):
    # Create output directory if it doesn't exist
    output_dir = 'csv_batches'
    os.makedirs(output_dir, exist_ok=True)

    # Read the CSV file to get the total number of records
    with open(input_file, 'r', encoding='utf-8') as f:
        total_records = sum(1 for line in f)

    # Calculate the number of records for each split
    split_records = total_records // 2

    # Split the CSV into two equal parts
    batch_number = 1
    with open(input_file, 'r', encoding='utf-8') as f:
        for i, chunk in enumerate(pd.read_csv(f, chunksize=split_records)):
            batch_filename = os.path.join(output_dir, f"india-nodes_batch{batch_number}.csv")
            chunk.to_csv(batch_filename, index=False)
            print(f"Batch {batch_number}/2 saved to {batch_filename}")
            batch_number += 1



def calculate_total_records(csv_dir):
    total_records = 0

    # Iterate over all files in the directory
    for filename in os.listdir(csv_dir):
        if filename.endswith(".csv"):
            # Construct the full path to the CSV file
            csv_file = os.path.join(csv_dir, filename)

            # Read the CSV file and count the number of records
            with open(csv_file, 'r', encoding='utf-8') as f:
                num_records = sum(1 for line in f)
                total_records += num_records

    return total_records

if __name__ == "__main__":
    input_file = "india-nodes.csv"  # Change this to your input CSV file
    total_records =  calculate_total_records("csv_batches")
    print(total_records)
    # split_csv(input_file)
