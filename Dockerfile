FROM jupyter/pyspark-notebook

RUN mkdir -p work/great_expectations
WORKDIR work/great_expectations
COPY requirements.txt ./requirements.txt
RUN pip install -r requirements.txt
RUN pip install jupyter

COPY . ./
RUN pip install .

RUN pytest
