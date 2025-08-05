#!/bin/bash

set -e

echo "Checking MySQL connection for Airflow..."
mysqladmin -h host.docker.internal -P 3306 -u airflow -pairflow ping || { echo "MySQL ping failed"; exit 1; }

echo "Checking if Airflow database is initialized..."
if airflow db check; then
    echo "Airflow database already initialized, skipping init..."
else
    echo "Running airflow db init..."
    airflow db init || { echo "airflow db init failed"; exit 1; }
fi

echo "Running airflow db migrate..."
airflow db migrate || { echo "airflow db migrate failed"; exit 1; }

echo "Creating default connections..."
airflow connections create-default-connections

echo "Checking if admin user exists..."
if airflow users list | grep -q "admin"; then
    echo "Admin user already exists, skipping creation..."
else
    echo "Creating admin user..."
    airflow users create \
        --username admin \
        --password admin \
        --firstname Admin \
        --lastname Admin \
        --role Admin \
        --email admin@example.com || { echo "Admin user creation failed"; exit 1; }
fi

echo "Checking DAG file presence..."
ls -l /opt/airflow/dags || { echo "DAGs directory not found"; exit 1; }
if [ -f /opt/airflow/dags/etl_pipeline.py ]; then
    echo "etl_pipeline.py found, validating syntax..."
    python /opt/airflow/dags/etl_pipeline.py || { echo "Syntax error in etl_pipeline.py"; exit 1; }
else
    echo "etl_pipeline.py not found in /opt/airflow/dags"
    exit 1
fi

echo "Starting webserver and scheduler in background..."
airflow webserver & airflow scheduler &

echo "Waiting for scheduler to scan DAGs (60 seconds)..."
sleep 60

echo "Checking if etl_pipeline is registered..."
airflow dags list | grep etl_pipeline || { echo "etl_pipeline not registered in DagModel"; exit 1; }

echo "Triggering etl_pipeline DAG..."
airflow dags trigger etl_pipeline || { echo "Failed to trigger etl_pipeline DAG"; exit 1; }

echo "Keeping container running..."
wait