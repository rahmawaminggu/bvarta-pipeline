FROM eclipse-temurin:21-jre-jammy

# Python 3.12
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3.12 python3.12-venv python3-pip \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install dependencies before copying source (better layer caching)
COPY pyproject.toml ./
RUN python3.12 -m pip install --no-cache-dir "pyspark>=4.0.0" pyyaml pytest pytest-cov

# Copy project
COPY src/       ./src/
COPY job/       ./job/
COPY config/    ./config/
COPY data/      ./data/
COPY tests/     ./tests/

ENV PYTHONPATH="/app/src"
ENV JAVA_HOME="/opt/java/openjdk"

CMD ["python3.12", "job/pipeline.py", "--config", "config/pipeline.yaml"]