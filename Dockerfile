FROM quay.io/astronomer/astro-runtime:13.2.0

WORKDIR /usr/local/airflow

COPY python_packages.txt .

RUN pip install --no-cache-dir --upgrade pip setuptools wheel \
 && pip install --no-cache-dir -r python_packages.txt
