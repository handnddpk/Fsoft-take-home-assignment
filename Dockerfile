FROM apache/airflow:2.8.1-python3.11

# Switch to root user to install system dependencies
USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Copy requirements and install Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Create necessary directories
RUN mkdir -p /opt/airflow/data/input \
    && mkdir -p /opt/airflow/data/output \
    && mkdir -p /opt/airflow/data/archive \
    && mkdir -p /opt/airflow/dags/src \
    && mkdir -p /opt/airflow/logs

# Copy source code and configuration files
COPY src/ /opt/airflow/dags/src/
COPY airflow/dags/ /opt/airflow/dags/
COPY data/input/ /opt/airflow/data/input/
COPY sql/ /opt/airflow/sql/

# Set environment variables
ENV AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=false
ENV AIRFLOW__CORE__LOAD_EXAMPLES=false
ENV AIRFLOW__WEBSERVER__EXPOSE_CONFIG=true
ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor
ENV AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////opt/airflow/airflow.db

# Set working directory
WORKDIR /opt/airflow
