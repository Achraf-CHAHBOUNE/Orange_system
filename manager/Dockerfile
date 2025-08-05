# Use the official Airflow 2.7.3 image
FROM apache/airflow:2.7.3

# Switch to root to copy, set permissions, and install packages
USER root

# Copy entrypoint script, DAGs, and utils directories
COPY entrypoint.sh /entrypoint.sh
COPY dags/ /opt/airflow/dags/
COPY utils/ /opt/airflow/dags/utils/

# Make entrypoint script executable and set permissions
RUN chmod +x /entrypoint.sh && \
    chmod -R 755 /opt/airflow/dags && \
    chmod -R 755 /opt/airflow/dags/utils

# Install Docker CLI
RUN apt-get update && \
    apt-get install -y docker.io && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Debug: List DAGs and utils directory contents
RUN ls -l /opt/airflow/dags && \
    ls -l /opt/airflow/dags/utils

# Switch back to airflow user for runtime
USER airflow

# Install pymysql and apache-airflow-providers-docker
RUN pip install --no-cache-dir pymysql==1.1.0 apache-airflow-providers-docker==3.4.0

# Set entrypoint
ENTRYPOINT ["/entrypoint.sh"]