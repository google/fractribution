FROM python:3.7-slim

WORKDIR .
COPY . /

RUN pip install --upgrade functions-framework
RUN pip install --upgrade google-api-python-client oauth2client
RUN pip install --upgrade google-auth
RUN pip install --upgrade google-auth-httplib2
RUN pip install --upgrade google-cloud-bigquery
RUN pip install --upgrade google-cloud-logging
RUN pip install --upgrade google-cloud-pubsub
RUN pip install --upgrade jinja2

ENTRYPOINT python start.py
