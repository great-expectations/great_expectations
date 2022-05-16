FROM --platform=linux/amd64 python:3.9-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app
RUN apt-get update
RUN apt-get install -y git build-essential

# deps for mssql
RUN apt-get install -y curl apt-transport-https debconf-utils gcc
# Add mssql repo
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update
# mssql driver
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql17 mssql-tools

# used by requirements.txt: pyodbc, and enables mssql dialect
RUN apt-get install -y unixodbc-dev
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . /app/
RUN yes | great_expectations init
CMD pytest test_ge.py -vvv
