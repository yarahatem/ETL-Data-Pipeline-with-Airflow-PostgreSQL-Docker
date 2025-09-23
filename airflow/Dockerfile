FROM apache/airflow:2.7.2

# Set the working directory inside the container
WORKDIR /opt/airflow

# Copy requirements.txt from host into the container
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt