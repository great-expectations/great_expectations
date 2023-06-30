FROM busybox

# Create a non-root user to own the files and run our server
RUN adduser -D static
USER static
WORKDIR /gx/gx_stores/data_docs/

CMD ["busybox", "httpd", "-f", "-v", "-p", "3000"]
