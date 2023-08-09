# Use a base image with PySpark and Spark pre-installed
FROM bitnami/spark:latest

# Set the working directory inside the container
WORKDIR /app

# Change the user and group for the container process
USER root:root

# Copy the application code and data into the container
COPY . /app

# Change the ownership of the copied files
RUN chown -R root:root /app

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Change back to the original user
USER 1001:1001

# Command to run the data pipeline script
CMD ["python", "scripts/data_pipeline.py"]
