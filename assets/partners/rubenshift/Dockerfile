FROM --platform=linux/amd64 python:3.9-slim
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt
RUN yes | great_expectations init
COPY . /app/
