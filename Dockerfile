FROM apache/airflow:2.3.0
COPY ./requirement.txt ./requirement.txt
RUN pip install -r requirement.txt