FROM python:3-alpine
MAINTAINER henry.rizzi@adops.com

RUN apk update && \
apk add --update build-base \
libffi-dev \
postgresql-dev \
postgresql


RUN rm -rf /var/cache/apk/* && rm -rf /tmp/*

ADD . /app
WORKDIR /app

RUN pip install luigi mock nose

RUN cd /app && python3 setup.py install

RUN su postgres -c nosetests
