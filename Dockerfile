# Use the official Airflow image as the base image
FROM apache/airflow:2.10.0

# Switch to root to install Python packages
USER airflow

# Copy your requirements.txt file to the Docker image
COPY requirements.txt .

# Install the packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# # Switch back to the airflow user
# USER airflow