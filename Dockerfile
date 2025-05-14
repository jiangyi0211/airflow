FROM quay.io/astronomer/astro-runtime:12.8.0

# 临时切换为 root 用户，安装 pandas 构建依赖
USER root

RUN apt-get update && \
    apt-get install -y gcc g++ build-essential && \
    rm -rf /var/lib/apt/lists/*

# 切回 airflow 用户（或 astro，具体取决于基础镜像）
USER astro

#安装 Soda 到虚拟环境（无需 source）
RUN python -m venv /usr/local/airflow/soda_venv && \
    /usr/local/airflow/soda_venv/bin/pip install --upgrade pip setuptools wheel && \
    /usr/local/airflow/soda_venv/bin/pip install "pandas<2.0.0" && \
    /usr/local/airflow/soda_venv/bin/pip install --no-cache-dir soda-core-bigquery==3.0.45 && \
    /usr/local/airflow/soda_venv/bin/pip install --no-cache-dir soda-core-scientific==3.0.45

ENV PATH="/usr/local/airflow/soda_venv/bin:$PATH"

# ---------------------
# Step 3: 安装 dbt-bigquery 到独立虚拟环境
# ---------------------
RUN python -m venv /usr/local/airflow/dbt_venv && \
    /usr/local/airflow/dbt_venv/bin/pip install --upgrade pip setuptools && \
    /usr/local/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-bigquery>=1.5.3

ENV PATH="/usr/local/airflow/dbt_venv/bin:$PATH"