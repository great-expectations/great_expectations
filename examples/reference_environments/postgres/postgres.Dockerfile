# Code and comments adapted from: https://cadu.dev/creating-a-docker-image-with-database-preloaded/

# dump build stage
FROM postgres:15.3-alpine as dumper

COPY db_dump.sql /docker-entrypoint-initdb.d/

# remove the exec "$@" content that exists in the docker-entrypoint.sh file
# so it will not start the PostgreSQL daemon (we don’t need it on this step).
RUN ["sed", "-i", "s/exec \"$@\"/echo \"skipping...\"/", "/usr/local/bin/docker-entrypoint.sh"]

ENV POSTGRES_USER=example_user
ENV POSTGRES_HOST_AUTH_METHOD=trust
ENV POSTGRES_DB=gx_example_db
# tell PostgreSQL to use /data as data folder, so we can copy it in the next step.
ENV PGDATA=/data

# Execute the entrypoint itself. It will execute the dump and load the data into /data folder.
# Since we executed the sed command to remove the $@ content it will not run the PostgreSQL daemon.
RUN ["/usr/local/bin/docker-entrypoint.sh", "postgres"]

# final build stage
FROM postgres:15.3-alpine

# This will copy all files from /data folder from the dumper step into the $PGDATA from this current step,
# making our data preloaded when we start the container
# (without needing to run the dump every time we create a new container).
COPY --from=dumper /data $PGDATA
