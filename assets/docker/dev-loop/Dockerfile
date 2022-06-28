FROM python:3.9-bullseye
COPY ../../../requirements-dev-lite.txt requirements-dev-lite.txt
COPY ../../../constraints-dev.txt constraints-dev.txt
RUN pip install --upgrade pip==21.3.1
RUN pip install -r requirements-dev-lite.txt -c constraints-dev.txt
RUN pip install great_expectations
RUN useradd -m ge_user && chown ge_user /ge
USER ge_user
RUN echo 'alias ge="great_expectations"' >> ~/.bashrc
RUN echo 'cat /ge/welcome.txt' >> ~/.bashrc
COPY services.py services.py
COPY welcome.txt welcome.txt
ENV GE_CONTAINER_WORKFLOW=True
ENV GE_JUPYTER_CMD="jupyter notebook --ip=0.0.0.0 --port=5051 --no-browser"
CMD ["python", "services.py"]
