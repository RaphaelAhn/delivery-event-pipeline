# Airflow 이미지 + 파이프라인 실행 도구(dbt, PySpark).
#
# dbt·PySpark 는 Airflow 와 같은 Python 환경에 넣지 않고 별도 venv(/opt/pipeline-venv)에 둔다.
# 한 환경에 섞으면 두 쪽의 의존성 버전이 부딪혀 Airflow 가 깨지기 쉽다.
# DAG 은 BashOperator 로 이 venv 의 명령만 호출한다.
FROM apache/airflow:3.3.2-python3.11

USER root
# PySpark 는 JVM 위에서 돈다. PATH 의 java 를 그대로 쓰므로 JAVA_HOME 은 두지 않는다.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*
RUN python -m venv /opt/pipeline-venv && chown -R airflow: /opt/pipeline-venv

USER airflow
# pyspark 버전은 scripts/spark_submit 이 쓰는 apache/spark:3.5.3 이미지와 맞춘다
RUN /opt/pipeline-venv/bin/pip install --no-cache-dir \
    "pyspark==3.5.3" \
    "dbt-clickhouse>=1.10,<1.11" \
    "clickhouse-connect>=0.8" \
    "confluent-kafka>=2.6" \
    "jsonschema>=4.23" \
    "python-dotenv>=1.0"

# spark-submit 이 드라이버를 띄울 때 Airflow 의 python 이 아니라 이 venv 를 쓰게 한다
ENV PYSPARK_PYTHON=/opt/pipeline-venv/bin/python
