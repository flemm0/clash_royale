FROM python:3.10-slim

# Install git

RUN apt-get update && \
    apt-get install -y git && \
    rm -rf /var/lib/apt/lists/*

# Upgrade pip

RUN pip install --upgrade pip

# Checkout and install dagster libraries needed to run the gRPC server
# exposing your repository to dagster-webserver and dagster-daemon, and to load the DagsterInstance

RUN pip install \
    dagster \
    dagster-postgres \
    dagster-docker

# Add repository code

WORKDIR /opt/dagster/app

COPY . /opt/dagster/app

COPY .env /opt/dagster/app/

RUN pip install -r requirements.txt

# Run dagster gRPC server on port 4000

EXPOSE 4000

# CMD allows this to be overridden from run launchers or executors that want
# to run other commands against your repository
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "clash_royale_etl_project"]
