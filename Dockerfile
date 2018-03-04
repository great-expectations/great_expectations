FROM python:3

RUN mkdir -p /great_expectations
WORKDIR /great_expectations
COPY requirements.txt ./requirements.txt
RUN pip install -r requirements.txt
RUN pip install jupyter

COPY . ./
RUN pip install .

RUN python tester.py