# Use Airflow image as base to ensure compatibility with Airflow orchestration
FROM apache/airflow:2.9.0-python3.12

USER root

# Install system dependencies if any
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy project files
COPY --chown=airflow:root pyproject.toml /opt/airflow/
COPY --chown=airflow:root src/ /opt/airflow/src/
COPY --chown=airflow:root dags/ /opt/airflow/dags/
COPY --chown=airflow:root config/ /opt/airflow/config/
COPY --chown=airflow:root README.md /opt/airflow/


# Install the project
RUN pip install --no-cache-dir -e ".[airflow]"

WORKDIR /opt/airflow
