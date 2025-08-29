FROM apache/airflow:2.11.0-python3.11
RUN pip install --no-cache-dir great_expectations psycopg2-binary pandas matplotlib seaborn
USER airflow