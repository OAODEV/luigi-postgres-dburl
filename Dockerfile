FROM python:3-alpine
LABEL maintainer="henry.rizzi@adops.com"

RUN apk add --no-cache build-base \
libffi-dev \
postgresql-dev \
postgresql

ADD . /app
WORKDIR /app

RUN pip install luigi mock nose

RUN cd /app && python3 setup.py install
