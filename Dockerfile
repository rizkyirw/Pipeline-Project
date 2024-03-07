FROM apache/airflow:2.6.1-python3.9

USER root

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
  chromium \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
  selenium==4.12.0 \
  webdriver-manager==4.0.0
