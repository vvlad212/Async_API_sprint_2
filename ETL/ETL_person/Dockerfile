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

ENV PYTHONPATH "${PYTHONPATH}:/etl"

CMD [ "python", "./upload_elastick.py"]
