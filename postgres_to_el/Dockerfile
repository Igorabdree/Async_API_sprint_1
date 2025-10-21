FROM python:3.10-slim

WORKDIR /opt/app/etl

COPY ../requirements.txt .


RUN  pip3 install  --no-cache-dir -r requirements.txt


COPY ./etl/ .


ENTRYPOINT ["python", "app.py"]
