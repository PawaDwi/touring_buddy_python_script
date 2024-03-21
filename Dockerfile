# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed dependencies specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Define default values for parameters
ENV SOURCE_FILE_NAME="india-nodes.csv"
ENV DESTINATION_FILE_NAME="india-nodes.osm"

# Run the Python script with parameters when the container launches
CMD ["python", "app.py", "--source", "${SOURCE_FILE_NAME}", "--destination", "${DESTINATION_FILE_NAME}"]
