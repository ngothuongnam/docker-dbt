FROM ubuntu:22.04
RUN apt update -y \
    && apt upgrade -y \
    && apt install curl unixodbc-dev -y \
    && apt install python3-pip -y \
    && pip install --upgrade pip setuptools wheel \
    && pip install dbt-sqlserver \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18

