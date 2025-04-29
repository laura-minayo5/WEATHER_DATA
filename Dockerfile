# Use the official Airflow image as the base
FROM apache/airflow:latest

# COPY requirements.txt /

# Install the Docker provider for Airflow
RUN pip install apache-airflow-providers-docker
# RUN pip install --upgrade pip

# RUN pip install --no-cache-dir -r /requirements.txt

# # FROM mcr.microsoft.com/powershell:lts-7.2-nanoserver-ltsc2022
# # add persistent python path (for local imports)
# # ENV AIRFLOW_HOME= "C:\Users\Admin\weather_data"
# # ENV PYTHONPATH= "${PYTHONPATH};${AIRFLOW_HOME}"
# ENV AIRFLOW_HOME = //C/Users/Admin/weather_data
# ENV PYTHONPATH "${PYTHONPATH}:${AIRFLOW_HOME}"