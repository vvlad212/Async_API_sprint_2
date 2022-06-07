FROM python:3.9-buster
COPY requirements.txt ./requirements.txt
RUN python -m pip install --upgrade pip
RUN pip install --upgrade setuptools
RUN pip install -r ./requirements.txt
ENV PYTHONPATH "${PYTHONPATH}:/functional"
COPY functional functional

