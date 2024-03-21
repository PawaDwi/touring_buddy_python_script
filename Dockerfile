# Use an official Python runtime as a parent image
FROM python:3.8-slim

# Set environment variables
ENV PARAMETER_1=default_value \
    PARAMETER_2=default_value

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Run the Python script with parameters passed as arguments
CMD ["python", "script.py", "--param1=$PARAMETER_1", "--param2=$PARAMETER_2"]
