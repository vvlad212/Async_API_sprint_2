# syntax=docker/dockerfile:1
FROM python:3.10-alpine

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1


WORKDIR /etl

RUN python -m pip install --upgrade pip
RUN pip install --upgrade setuptools 
COPY requirements.txt /etl/
RUN \
 apk add --no-cache python3 postgresql-libs && \
 apk add --no-cache --virtual .build-deps gcc python3-dev musl-dev postgresql-dev && \
 python3 -m pip install -r requirements.txt --no-cache-dir && \
 apk --purge del .build-deps


COPY . /etl/


RUN crontab ./crontask/crontab

RUN touch /tmp/out.log /tmp/err.log

CMD crond && tail -f /tmp/out.log
