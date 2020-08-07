FROM ubuntu:latest

RUN apt-get update && apt-get install -y python3-pip git libturbojpeg0-dev
RUN pip3 install virtualenv

WORKDIR /opt/readthedocs
COPY rtd_build.sh /opt/rtd_build.sh
COPY rtd_conf_supplement.py /opt/rtd_conf_supplement.py

CMD ["/opt/rtd_build.sh"]
