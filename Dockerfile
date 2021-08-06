FROM python:3.7.11-buster AS base
# Setup dependencies for pyodbc
RUN \
  export ACCEPT_EULA='Y' && \
  export MYSQL_CONNECTOR='mysql-connector-odbc-8.0.18-linux-glibc2.12-x86-64bit' && \
  export MYSQL_CONNECTOR_CHECKSUM='f2684bb246db22f2c9c440c4d905dde9' && \
  apt-get update && \
  apt-get install -y curl build-essential unixodbc-dev g++ apt-transport-https && \
#  gpg --keyserver hkp://keys.gnupg.net --recv-keys 5072E1F5 && \
  #
  # Install pyodbc db drivers for MSSQL, PG and MySQL
  curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
  curl https://packages.microsoft.com/config/debian/9/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
  curl -L -o ${MYSQL_CONNECTOR}.tar.gz https://dev.mysql.com/get/Downloads/Connector-ODBC/8.0/${MYSQL_CONNECTOR}.tar.gz && \
  curl -L -o ${MYSQL_CONNECTOR}.tar.gz.asc https://downloads.mysql.com/archives/gpg/\?file\=${MYSQL_CONNECTOR}.tar.gz\&p\=10 && \
#  gpg --verify ${MYSQL_CONNECTOR}.tar.gz.asc && \
  echo "${MYSQL_CONNECTOR_CHECKSUM} ${MYSQL_CONNECTOR}.tar.gz" | md5sum -c - && \
  apt-get update && \
  gunzip ${MYSQL_CONNECTOR}.tar.gz && tar xvf ${MYSQL_CONNECTOR}.tar && \
  cp ${MYSQL_CONNECTOR}/bin/* /usr/local/bin && cp ${MYSQL_CONNECTOR}/lib/* /usr/local/lib && \
  myodbc-installer -a -d -n "MySQL ODBC 8.0 Driver" -t "Driver=/usr/local/lib/libmyodbc8w.so" && \
  myodbc-installer -a -d -n "MySQL ODBC 8.0" -t "Driver=/usr/local/lib/libmyodbc8a.so" && \
  apt-get install -y msodbcsql17 odbc-postgresql && \
  #
  # Update odbcinst.ini to make sure full path to driver is listed
  sed 's/Driver=psql/Driver=\/usr\/lib\/x86_64-linux-gnu\/odbc\/psql/' /etc/odbcinst.ini > /tmp/temp.ini && \
  mv -f /tmp/temp.ini /etc/odbcinst.ini && \
  # Install dependencies
  pip install --upgrade pip && \
  # Cleanup build dependencies
  rm -rf ${MYSQL_CONNECTOR}* && \
  apt-get remove -y curl apt-transport-https debconf-utils g++ gcc rsync unixodbc-dev build-essential gnupg2 && \
  apt-get autoremove -y && apt-get autoclean -y


RUN mkdir -p /home/app
COPY . /home/app/
WORKDIR /home/app
RUN ls
# RUN apt-get install -y software-properties-common
# RUN add-apt-repository ppa:openjdk-r/ppa
# RUN apt-get update
RUN apt-get install -y default-jdk
RUN apt-get install -y gcc
RUN apt-get install -y unixodbc
RUN apt-get install -y python3-dev
RUN apt-get install -y build-essential
RUN apt-get install -y unixodbc-dev
RUN apt-get install -y locales
RUN sed -i '/en_US.UTF-8/s/^# //g' /etc/locale.gen && \
    locale-gen
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8
RUN pip install pypandoc
RUN pip install openpyxl
RUN pip install -r requirements-dev.txt -c constraints-dev.txt
RUN pip install .
ENTRYPOINT ["/home/app/test.sh"]
