FROM python:3.12-slim

# Install build dependencies for mysqlclient and numpy

RUN apt-get update && apt-get install -y gcc python3-dev libmariadb-dev libmariadb-dev-compat pkg-config && rm -rf /var/lib/apt/lists/*

WORKDIR /app 

COPY requirements.txt . 

RUN pip install --no-cache-dir -r requirements.txt 

COPY src/ . 
 
