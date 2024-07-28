# Use the official Python image as the base image
FROM python:3.10-alpine

# Install necessary packages
RUN apk add --no-cache gcc musl-dev linux-headers

# Copy requirements.txt to the container
COPY requirements.txt requirements.txt

# Install Python packages from requirements.txt
RUN pip install -r requirements.txt

# Copy the dags folder to the container
COPY dags /opt/airflow/dags

# Set the working directory
WORKDIR /opt/airflow

# Set the entrypoint to the Airflow entrypoint script
ENTRYPOINT ["airflow"]

# Set the default command to run the webserver
CMD ["webserver"]
