FROM ubuntu:18.04

RUN apt-get update && apt-get install -y python3-pip git libjpeg8-dev
RUN pip3 install virtualenv

WORKDIR /opt/readthedocs
COPY rtd_build.sh /opt/readthedocs/rtd_build.sh
COPY rtd_conf_supplement.py /opt/readthedocs/rtd_conf_supplement.py

ENTRYPOINT ["/opt/readthedocs/rtd_build.sh"]