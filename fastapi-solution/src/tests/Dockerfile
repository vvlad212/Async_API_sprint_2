FROM python:3.9-buster

COPY tests/requirements.txt ./requirements.txt
COPY api/errors ./api/errors

RUN python -m pip install --upgrade pip
RUN pip install --upgrade setuptools
RUN pip install -r ./requirements.txt

ENV PYTHONPATH "${PYTHONPATH}:/functional"

COPY tests/functional functional
