# Use the official Airflow image as the base image
FROM apache/airflow:2.10.0

# Switch to root to install Python packages
# USER root

# RUN mkdir -p /opt/airflow /opt/airflow/dags /opt/airflow/logs /opt/airflow/plugins /opt/airflow/config 
# RUN chown -R airflow:root /opt/airflow

# Copy your requirements.txt file to the Docker image


# Switch back to the airflow user
USER airflow
COPY requirements.txt .
# Install the packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt