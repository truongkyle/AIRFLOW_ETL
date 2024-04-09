FROM apache/airflow:2.3.0-python3.8

WORKDIR /opt/airflow

COPY requirements.txt .
RUN pip install -r requirements.txt

# CMD bash -c '(airflow db init && airflow users create --username admin --password admin --firstname Htd --lastname Coserv --role Admin --email kl@gmail.com); airflow webserver & airflow scheduler'