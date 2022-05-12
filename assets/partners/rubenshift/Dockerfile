FROM --platform=linux/amd64 python:3.9-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app

RUN apt-get update

RUN apt-get install -y git gnupg build-essential libpq-dev

# Only for e2e, spark requires jdk (jre is ~ same size)
RUN apt-get -y install git default-jdk

# deps for mssql
RUN apt-get install -y curl apt-transport-https debconf-utils 
# Add mssql repo
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update
# mssql driver
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql17

# used by requirements.txt: pyodbc, and enables mssql dialect
RUN apt-get install -y unixodbc-dev

RUN pip install great_expectations
