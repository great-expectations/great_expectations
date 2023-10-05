FROM python

# Create a non-root user to own the files and run our server
RUN adduser static
USER static
WORKDIR /gx/gx_stores/data_docs/

CMD ["python", "-m", "http.server", "3000"]
