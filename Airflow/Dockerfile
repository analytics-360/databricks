# Dockerfile
# Use the official Airflow image tag that matches your Airflow version
FROM apache/airflow:3.1.5

# Install packages from requirements.txt as the 'airflow' user
# This ensures consistent permissions


COPY requirements.txt /tmp/requirements.txt
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir -r /tmp/requirements.txt