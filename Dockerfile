# Use the official Airflow image as the base image
FROM apache/airflow:2.10.0

# Switch user to airflow user
USER airflow

# Copy requirements.txt file to the Docker image
COPY requirements.txt .

# Install the packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt