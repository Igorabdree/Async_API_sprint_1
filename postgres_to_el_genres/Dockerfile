FROM python:3.10-slim

WORKDIR /opt/app/etl_g

COPY ../requirements.txt .


RUN  pip3 install  --no-cache-dir -r requirements.txt


COPY ./etl_g/ .


ENTRYPOINT ["python", "app.py"]
