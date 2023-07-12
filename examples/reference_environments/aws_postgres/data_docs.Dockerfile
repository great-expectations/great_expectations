FROM python

# Create a non-root user (airflow) to own the files and run our server
# Set the permissions on the data docs folder to be owned by airflow and available to all users.
USER root
RUN mkdir -p /gx/gx_stores/data_docs/
RUN adduser -U -u 1000 airflow
RUN chown -R airflow:0 /gx && chmod -R 777 /gx
USER airflow

WORKDIR /gx/gx_stores/data_docs/

CMD ["python", "-m", "http.server", "3000"]
