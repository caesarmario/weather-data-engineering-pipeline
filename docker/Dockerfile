####
## Dockerfile for Custom Airflow Image
## Mario Caesar // caesarmario87@gmail.com
####

FROM apache/airflow:3.0.2
COPY /docker/requirements.txt /requirements.txt
USER airflow
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /requirements.txt